package com.szadowsz.spark.ml.feature

import com.szadowsz.common.lang.distance.LevenshteinDistance
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created on 28/11/2016.
  */
class CatSpellingTransformer(override val uid: String) extends UnaryTransformer[String, String, CatSpellingTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val dictionary: Param[Map[String,String]] = new Param[Map[String,String]](this, "dictionary", "")

  protected var b : Broadcast[Map[String,String]] = _

  protected val limit: Param[Int] = new Param[Int](this, "limit", "")

  def setDictionary(dict: Map[String,String]): this.type = {
    set("dictionary", dict)
  }

  def getOutputCols: Map[String,String] = $(dictionary)

  def setLimit(l: Int): this.type = {
    set("limit", l)
  }

  def getLimit: Int = $(limit)


  override protected def createTransformFunc: (String) => String = (s: String) => {
    Option(s) match {
      case Some(x) =>
          val (result, _) = b.value.keys.filter(_!=null).foldLeft((null.asInstanceOf[String], Int.MaxValue)) {
            case ((rep, best), curr) =>
              val dist = LevenshteinDistance.difference(x, curr).toInt
              if (dist < best && dist <= $(limit)) (b.value(curr), dist) else (rep, best)
          }
          result
      case None => $(dictionary).getOrElse(null,null)
    }
  }

  override protected def outputDataType: DataType = StringType

  override def transform(dataset: Dataset[_]): DataFrame = {
    b = dataset.sparkSession.sparkContext.broadcast(${dictionary})
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    super.transform(dataset)
  }
}

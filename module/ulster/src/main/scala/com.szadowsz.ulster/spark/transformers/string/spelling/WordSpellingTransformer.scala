package com.szadowsz.ulster.spark.transformers.string.spelling

import com.szadowsz.common.lang.distance.LevenshteinDistance
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created on 28/11/2016.
  */
class WordSpellingTransformer(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], WordSpellingTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val dictionary: Param[Array[String]] = new Param[Array[String]](this, "dictionary", "")

  protected var b : Broadcast[Array[String]] = _

  def setDictionary(dict: Seq[String]): this.type = {
    set("dictionary", dict.toArray)
  }

  def getOutputCols: Array[String] = $(dictionary)


  override protected def createTransformFunc: (Seq[String]) => Seq[String] = {
    (s: Seq[String]) => {
      Option(s) match {
        case Some(x) => s.map {
          w =>
            val (result, _) = b.value.foldLeft(("", 100)) {
              case ((rep, best), curr) =>
                val dist = LevenshteinDistance.difference(w, curr).toInt
                if (dist < best) (curr, dist) else (rep, best)
            }
            result
        }
        case None => null
      }
    }
  }

  override protected def outputDataType: DataType = ArrayType(StringType)

  override def transform(dataset: Dataset[_]): DataFrame = {
    b = dataset.sparkSession.sparkContext.broadcast(${dictionary})
    logger.info("Executing stage {}", uid)
    logger.debug("Processing dataset {}", dataset.schema.fieldNames.mkString("[", ",", "]"))
   super.transform(dataset)
  }
}

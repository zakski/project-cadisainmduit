package com.szadowsz.ulster.spark.transformers.string

import com.szadowsz.common.lang.WordTokeniser
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created on 28/11/2016.
  */
class TokeniserTransformer (override val uid : String) extends UnaryTransformer[String, Seq[String], TokeniserTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  override protected def createTransformFunc: (String) => Seq[String] = (s : String) => Option(s) match {
    case Some(_) => WordTokeniser.tokenise(s)
    case None => null
  }

  override protected def outputDataType: DataType = ArrayType(StringType)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    super.transform(dataset)
  }
}

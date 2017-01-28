package com.szadowsz.ulster.spark.transformers.string

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StringType}
import org.slf4j.LoggerFactory

/**
  * Created on 20/04/2016.
  */
class InitialisationTransformer(override val uid: String) extends UnaryTransformer[String, String, InitialisationTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  def this() = this(Identifiable.randomUID("iniT"))

  override protected def createTransformFunc: (String) => String = (n: String) => {
    Option(n) match {
      case Some(s) => s.charAt(0).toUpper.toString
      case None => null
    }
  }

    override protected def outputDataType: DataType = StringType

    override protected def validateInputType(inputType: DataType): Unit =
    {
      require(inputType == StringType, s"Input type must be string type but got $inputType.")
    }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    super.transform(dataset)
  }
}

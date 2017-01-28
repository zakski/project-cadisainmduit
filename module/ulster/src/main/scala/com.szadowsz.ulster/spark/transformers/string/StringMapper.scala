package com.szadowsz.ulster.spark.transformers.string

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamValidators}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  * Created on 11/07/2016.
  */
class StringMapper(override val uid: String) extends UnaryTransformer[String, String, StringMapper] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val mode: Param[String] = new Param[String](this, "mode", "mapping mode", ParamValidators.inArray[String](Array("exact")))
  setDefault(mode,"exact")

  protected val mapping: Param[Map[String, String]] = new Param[Map[String, String]](this, "mapping", "mapping values")

  def setMode(mode: String): this.type = set("mode", mode)

  def getMode: String = $(mode)

  def setMapping(mapping: Map[String, String]): this.type = set("mapping", mapping)

  def setMapping(mapping: Set[String]): this.type = set("mapping", mapping.map(v => v -> v).toMap)

  def getMapping: Map[String, String] = $(mapping)

  override protected def createTransformFunc: (String) => String = {
    $(mode) match {
      case "exact" => (in: String) => $(mapping).get(in).orNull
    }
  }

  override protected def outputDataType: DataType = StringType

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    super.transform(dataset)
  }
}

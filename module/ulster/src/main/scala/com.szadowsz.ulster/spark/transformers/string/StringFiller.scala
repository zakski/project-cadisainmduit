package com.szadowsz.ulster.spark.transformers.string

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  * Created on 19/08/2016.
  */
class StringFiller(override val uid: String) extends Transformer {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val value: Param[String] = new Param[String](this, "value", "output value")

  protected val outputCol: Param[String] = new Param[String](this, "outputCol", "output Column")

  def setOutputCol(input: String): this.type = set("outputCol", input)

  def getOutputCol: String = $(outputCol)

  def setValue(input: String): this.type = set(value, input)

  def getValue: String = $(value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val fun = udf{() => $(value)}
    dataset.toDF().withColumn($(outputCol),fun())
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    logger.info("Executing stage {}",uid)
    StructType(schema :+ StructField($(outputCol),StringType))
  }
}

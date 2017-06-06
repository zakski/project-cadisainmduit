package com.szadowsz.ulster.spark.transformers

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 29/05/2017.
  */
abstract class OutOptUnaryTransformer[V, T <: UnaryTransformer[V, V, T]](override val uid: String) extends UnaryTransformer[V, V, T] {
  protected val logger : Logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  override def transformSchema(schema: StructType): StructType = {
    if (isSet(outputCol))
      StructType(schema :+ StructField($(outputCol), outputDataType))
    else
      StructType(schema.filterNot(f => f.name == $(inputCol)) :+ StructField($(inputCol), outputDataType))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(this.createTransformFunc, outputDataType)
    if (isSet(outputCol)) {
      dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
    } else {
      val df = dataset.withColumn(uid, transformUDF(dataset($(inputCol)))).drop($(inputCol)).withColumnRenamed(uid,$(inputCol))
      df
    }
  }
}

package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._

class RoundingTransformer(override val uid: String) extends Transformer with HasInputCol with HasOutputCol{

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.withColumn(${outputCol},round(col(${inputCol})))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = StructType(schema.fields :+ StructField(${outputCol},IntegerType))
}

package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{DoubleParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class MinMaxScaler(override val uid: String) extends Transformer with HasInputCol with HasOutputCol{

  def this() = this(Identifiable.randomUID("minMaxScal"))

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val summary = dataset.select(min(${inputCol}),max(${inputCol})).first().toSeq
    val minimum = summary.head.toString.toDouble
    val maximum = summary.last.toString.toDouble

    dataset.withColumn(${outputCol},(col(${inputCol}) - lit(minimum)) / lit(maximum))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema :+ StructField(${outputCol},DoubleType))
  }

  override def copy(extra: ParamMap): MinMaxScaler = defaultCopy(extra)
}
package com.szadowsz.ulster.spark.transformers.util

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created on 01/12/2016.
  */
case class PassThroughTransformer(override val uid: String, stage: Transformer) extends Transformer {

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.cache()
    stage.transform(dataset)
    dataset.toDF()
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema
}

package com.szadowsz.ulster.spark.transformers.math

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * Created on 26/01/2017.
  */
class NullTransformer(override val uid: String) extends Transformer {

  protected val replacement: Param[Double] = new Param[Double](this, "replacement", "replacement value")
  setDefault(replacement, Double.NaN)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {
    dataset.na.fill($(replacement))
  }

}
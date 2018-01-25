package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 26/01/2017.
  */
class NullReplacer(override val uid: String) extends Transformer with HasInputCols {
  protected val logger: Logger = LoggerFactory.getLogger("com.szadowsz.spark.ml")

  protected val replacement: Param[Any] = new Param[Any](this, "replacement", "replacement value")
  setDefault(replacement, Double.NaN)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    dataset.na.fill($(inputCols).map(_ -> $(replacement)).toMap)
  }
}

package com.szadowsz.ulster.spark.transformers

import com.szadowsz.ulster.spark.params.{HasInputCols, HasIsInclusive}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * Created on 25/01/2017.
  */
class ColFilterTransformer(override val uid: String) extends Transformer with HasInputCols with HasIsInclusive {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  def this() = this(Identifiable.randomUID("colSelect"))

  setDefault(isInclusive,false)

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(if ($(isInclusive)) schema.filter(f => $(inputCols).contains(f.name)) else schema.filterNot(f => $(inputCols).contains(f.name)))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    transformSchema(dataset.schema)
    if ($(isInclusive))
      dataset.select($(inputCols).head,$(inputCols).tail:_*)
    else
      dataset.drop($(inputCols):_*)
  }
}

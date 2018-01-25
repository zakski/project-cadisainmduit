package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 17/01/2017.
  */
class AverageTransformer(override val uid: String) extends Transformer with HasInputCols with HasOutputCol {
  protected val logger: Logger = LoggerFactory.getLogger("com.szadowsz.spark.ml")

  def this() = this(Identifiable.randomUID("avg"))

  override def transformSchema(schema: StructType): StructType = {
    val inputs = schema.filter(f => $(inputCols).contains(f.name))
    require(inputs.forall(s => s.dataType.isInstanceOf[NumericType]))

    StructType(schema.fields :+ StructField($(outputCol),DoubleType))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}", uid)
    dataset.withColumn($(outputCol),$(inputCols).map(c => col(c)).foldLeft(lit(0))(_ + _) / $(inputCols).length)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

package com.szadowsz.ulster.spark.transformers

import com.szadowsz.ulster.spark.params.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  * Created on 15/07/2016.
  */
class CastTransformer(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  protected val outputDataType: Param[DataType] = new Param(this, "outputDataType", "type to cast the input column to.")

  def this() = this(Identifiable.randomUID("castTransformer"))

  def setOutputDataType(input: DataType): this.type = set("outputDataType", input)

  def getOutputDataType: DataType = $(outputDataType)


  override def transformSchema(schema: StructType): StructType = {
    if (isDefined(outputCol))
    StructType(schema :+ StructField($(outputCol), $(outputDataType)))
    else
      StructType(schema.map(f => if(f.name == $(inputCol)) StructField($(inputCol), $(outputDataType)) else f))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    transformSchema(dataset.schema)
    val castName = if (isDefined(outputCol)) $(outputCol) else $(inputCol)
    dataset.withColumn(castName, dataset($(inputCol)).cast($(outputDataType)))
  }

  override def copy(extra: ParamMap): CastTransformer = defaultCopy(extra)
}


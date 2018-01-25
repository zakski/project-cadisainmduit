package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  * Created on 25/01/2017.
  */
class ColRenamerTransformer(override val uid: String) extends Transformer with HasInputCols {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  def this() = this(Identifiable.randomUID("colSelect"))

  protected val outputCols : Param[Array[String]] = new Param[Array[String]](this,"outputCols","",ParamValidators.arrayLengthGt[String](0))

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require($(inputCols).length == $(outputCols).length)
    StructType(schema.map(f => if($(inputCols).contains(f.name)) f.copy(name = $(outputCols)($(inputCols).indexOf(f.name))) else f))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    transformSchema(dataset.schema)
   $(inputCols).zip($(outputCols)).foldLeft(dataset.toDF()){case (df, (in,out)) => df.withColumnRenamed(in,out)}
  }
}

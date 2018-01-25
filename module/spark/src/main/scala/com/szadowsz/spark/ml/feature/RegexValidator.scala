package com.szadowsz.spark.ml.feature

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 02/12/2016.
  */
class RegexValidator(override val uid: String) extends Transformer with HasInputCol {
  protected val logger: Logger = LoggerFactory.getLogger("com.szadowsz.spark.ml")

  protected val pattern: Param[String] = new Param(this, "pattern", "regex pattern")

  def setPattern(value: String): this.type = set(pattern, value)

  def getPattern: String = $(pattern)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    val regex = $(pattern).r
    val func: (String => Boolean) = (arg: String) => {
      Option(arg) match {
        case Some(s) => regex.pattern.matcher(s).matches()
        case None => false
      }
    }
    val sqlfunc = udf(func)
    dataset.filter(sqlfunc(col($(inputCol)))).toDF()
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema
}

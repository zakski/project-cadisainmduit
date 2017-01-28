package com.szadowsz.ulster.spark.transformers.string.spelling

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory

/**
  * Created on 02/12/2016.
  */
class RegexValidationTransformer(override val uid: String) extends Transformer {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "input Column", { s: String => s != null && s.length > 0 })

  protected val pattern: Param[String] = new Param(this, "pattern", "regex pattern")

  def setInputCol(input: String): this.type = set(inputCol, input)

  def getInputCol: String = $(inputCol)

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

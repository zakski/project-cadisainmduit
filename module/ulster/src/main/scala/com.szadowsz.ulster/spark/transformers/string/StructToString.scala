package com.szadowsz.ulster.spark.transformers.string

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory


/**
  * Created on 01/07/2016.
  */
class StructToString(override val uid: String) extends UnaryTransformer[Seq[String], String, StructToString] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val start: Param[String] = new Param[String](this, "start", "prefox for array based output names")
  protected val sep: Param[String] = new Param[String](this, "sep", "prefox for array based output names")
  protected val end: Param[String] = new Param[String](this, "endt", "prefox for array based output names")


  setDefault(start, "")
  setDefault(sep, " ")
  setDefault(end, "")

  def this() = this(Identifiable.randomUID("structToString"))

  override protected def createTransformFunc: (Seq[String]) => String = (s: Seq[String]) => {
    Option(s) match {
      case Some(_) => s.mkString($(start), $(sep), $(end))
      case None => null
    }
  }

  def setStringStart(value: String): this.type = set(start, value)

  def getStringStart: String = $(start)

  def setStringSeparator(value: String): this.type = set(sep, value)

  def getStringSeparator: String = $(sep)

  def setStringEnd(value: String): this.type = set(end, value)

  def getStringEnd: String = $(end)

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema :+ StructField($(outputCol), StringType, true))
  }

  override protected def outputDataType: DataType = StringType

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    logger.debug("Processing dataset {}",dataset.schema.fieldNames.mkString("[",",","]"))
    super.transform(dataset)
  }
}
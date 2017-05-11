package com.szadowsz.ulster.spark.transformers

import java.io.StringReader

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created on 28/11/2016.
  */
class CsvTransformer(override val uid: String) extends Transformer {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "input Column", { s: String => s != null && s.length > 0 })

  protected val tmpCol: Param[String] = new Param[String](this, "tmpCol", "temporary Column", { s: String => s != null && s.length > 0 })

  protected val outputCols: Param[Array[String]] = new Param[Array[String]](this, "outputCols", "user defined output column names")

  protected val sizeParam: Param[Int] = new Param[Int](this, "size", "number of output columns")

  setDefault(tmpCol, "tmp")

  private def read: (String) => Seq[String] = (row: String) => new CsvListReader(new StringReader(row), CsvPreference.STANDARD_PREFERENCE).read().asScala

  private def createFunct(index: Int) = {
    udf { a: mutable.WrappedArray[String] => if (a.length > index) a(index) else null.asInstanceOf[String] }
  }

  private def createName(index: Int): String = $(outputCols)(index)

  def setOutputCols(outputCols: Seq[String]): CsvTransformer = {
    set("outputCols", outputCols.toArray)
  }

  def getOutputCols: Array[String] = $(outputCols)

  def setSize(size: Int): CsvTransformer = set(sizeParam, size)

  def getSize: Int = $(sizeParam)

  def setInputCol(input: String): CsvTransformer = set(inputCol, input)

  def getInputCol: String = $(inputCol)

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}", uid)
    logger.debug("Processing dataset {}", dataset.schema.fieldNames.mkString("[", ",", "]"))
    val transformUDF = udf(read, ArrayType(StringType))
    val tmp = dataset.withColumn($(tmpCol), transformUDF(dataset($(inputCol))))
    val result = (0 until $(sizeParam)).foldLeft(tmp.toDF()) { case (df, i) => df.withColumn(createName(i), createFunct(i)(df($(tmpCol)))) }
    result.cache()
    result.drop($(tmpCol), $(inputCol))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    new StructType($(outputCols).map(n => StructField(n, StringType)))
  }
}

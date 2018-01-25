package com.szadowsz.spark.ml.feature

import java.io.StringReader

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasInputCol
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.slf4j.{Logger, LoggerFactory}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created on 28/11/2016.
  */
class CsvColumnExtractor(override val uid: String) extends Transformer with HasInputCol {

  protected val logger: Logger = LoggerFactory.getLogger("com.szadowsz.spark.ml")

  protected val delimiter: Param[Char] = new Param[Char](this, "delimiter", "")

  protected val tmpCol: Param[String] = new Param[String](this, "tmpCol", "temporary Column", { s: String => s != null && s.length > 0 })

  protected val outputCols: Param[Array[String]] = new Param[Array[String]](this, "outputCols", "user defined output column names")

  protected val sizeParam: Param[Int] = new Param[Int](this, "size", "number of output columns")

  setDefault(tmpCol, "tmp")
  setDefault(delimiter, ',')

  private def read: (String) => Seq[String] = (row: String) => {
    val pref = new CsvPreference.Builder('"',$(delimiter), "\r\n").build
    new CsvListReader(new StringReader(row), pref).read().asScala
  }

  private def createFunct(index: Int) = {
    udf { a: mutable.WrappedArray[String] => if (a.length > index) a(index) else null.asInstanceOf[String] }
  }

  private def createName(index: Int): String = $(outputCols)(index)

  def setInputCol(name : String) : CsvColumnExtractor = {
    set(inputCol,name)
  }

  def setOutputCols(output: Seq[String]): CsvColumnExtractor = {
    set(outputCols, output.toArray)
  }

  def getOutputCols: Array[String] = $(outputCols)

  def setSize(size: Int): CsvColumnExtractor = set(sizeParam, size)

  def setDelimiter(tabDelimited: Char) = set(delimiter, tabDelimited)

  def getDelimiter: Char = $(delimiter)

  def getSize: Int = $(sizeParam)

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

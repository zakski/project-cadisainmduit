package com.szadowsz.spark.ml.feature

import com.szadowsz.common.io.write.CsvWriter
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, _}

/**
  * Created on 01/12/2016.
  */
class CrossTabTransformer(override val uid: String) extends Transformer {

  protected val isDebug: Param[Boolean] = new Param[Boolean](this, "isDebug", "flag for outputting debugging info")

  protected val debugPath: Param[String] = new Param[String](this, "debugPath", "filepath for debug info")

  protected val inputCols: Param[Array[String]] = new Param[Array[String]](this, "inputCols", "input Columns", (v: Array[String]) => v.length == 2)

  def setInputCols(inputs: String*): this.type = set("inputCols", inputs.toArray)

  def setDebugMode(enableDebug: Boolean): this.type = set("isDebug", enableDebug)

  def setDebugPath(path: String): this.type = set("debugPath", path)

  def this() = this(Identifiable.randomUID("tabStats"))

  protected final def genFileName(c1: String, c2: String) = s"$uid-$c1-tab-$c2.csv"

  protected def writeDistinctCounts(folder: String, column: String, list: List[(Any, Long)]): Unit = {
    val writer = new CsvWriter(folder + column + ".csv", "UTF-8", false)

    list.sortBy { case (key, value) => (-value, key.toString) }.foreach { case (key, value) =>
      writer.write(List(key.toString, value.toString))
    }

    writer.close()
  }

  protected def write(folder: String, c1: String, c2: String, schema: StructType, rows: Seq[Row]): Unit = {
    val writer = new CsvWriter(folder + genFileName(c1, c2), "UTF-8", false)
    writer.write(schema.fieldNames.toSeq)
    writer.writeAll(rows.sortBy(r => r.getString(0)).map(r => r.toSeq.map(Option(_).map(_.toString).getOrElse(""))))
    writer.close()
  }

  override def transformSchema(schema: StructType): StructType = schema

  override def transform(dataset: Dataset[_]): DataFrame = {

    val Array(c1, c2) = $(inputCols)

    val result = dataset.stat.crosstab(c1, c2)

    if (isDefined(debugPath)) {
      val folder = $(debugPath) + uid + "/"
      write(folder, c1, c2, result.schema, result.collect())
    }
    result
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

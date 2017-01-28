package com.szadowsz.ulster.spark.transformers.util.stats

import com.szadowsz.ulster.spark.expr._
import com.szadowsz.common.io.write.CsvWriter
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}


/**
  * Created on 27/04/2016.
  */
class StringStatistics(override val uid: String) extends Transformer {

  protected val summaryFileSuffix: String = "-string-categorical-summary"

  protected val isDebug: Param[Boolean] = new Param[Boolean](this, "isDebug", "flag for outputting debugging info")

  protected val debugPath: Param[String] = new Param[String](this, "debugPath", "filepath for debug info")

  protected val inputCols: Param[Array[String]] = new Param[Array[String]](this, "inputCols", "input Columns",ParamValidators.arrayLengthGt[String](0))

  protected val count: Param[Boolean] = new Param[Boolean](this, "count", "whether to count total non-nulls")

  protected val distinct: Param[Boolean] = new Param[Boolean](this, "distinct", "whether to count total non-null distinct values")

  protected val unique: Param[Boolean] = new Param[Boolean](this, "unique", "whether to count total non-null singleton values")

  protected val underThresholds: Param[List[Long]] = new Param[List[Long]](this, "bthresholds", "whether to count total non-null singleton values")

  protected val overThresholds: Param[List[Long]] = new Param[List[Long]](this, "athresholds", "whether to count total non-null singleton values")

  protected val rangeThresholds: Param[List[Range]] = new Param[List[Range]](this, "rthresholds", "whether to count total non-null singleton values")

  setDefault(
    ParamPair(count, true),
    ParamPair(distinct, true),
    ParamPair(unique, false),
    ParamPair(underThresholds, Nil),
    ParamPair(overThresholds, Nil),
    ParamPair(rangeThresholds, Nil)
  )

  def setShouldUseCount(shouldUseCount: Boolean): this.type = set("count", shouldUseCount)

  def setShouldUseDistinct(shouldUseDistinct: Boolean): this.type = set("distinct", shouldUseDistinct)

  def setShouldUseUnique(shouldUseUnique: Boolean): this.type = set("unique", shouldUseUnique)

  def setBelowThresholds(thresholds: List[Long]): this.type = set("bthresholds", thresholds.sorted)

  def setAboveThresholds(thresholds: List[Long]): this.type = set("athresholds", thresholds.sorted)

  def setThresholdRanges(thresholds: List[Range]): this.type = set("rthresholds", thresholds.sortBy(_.start))

  def setInputCols(inputs: Seq[String]): this.type = set("inputCols", inputs.toArray)

  def setDebugMode(enableDebug: Boolean): this.type = set("isDebug", enableDebug)

  def setDebugPath(path: String): this.type = set("debugPath", path)

  /**
    * Method to determine statistical columns to provide based on user defined params.
    *
    * @return list of (column name - stats filter) tuples
    */
  protected def buildUserDefinedStats: List[(String, Map[Any, Long] => Long)] = {
    var stats = List[(String, Map[Any, Long] => Long)]()

    // add the list of user defined category appearance thresholds so that we can count the number of categories that are over them.
    stats = $(overThresholds).map(t => (">" + t) -> ((m: Map[Any, Long]) => m.count(_._2 > t).toLong)) ::: stats

    // add the list of user defined category appearance ranges so that we can count the number of categories that are between the bounds.
    stats = $(rangeThresholds).map(r => (r.start + "-" + r.last) ->
      ((m: Map[Any, Long]) => m.count(kv => kv._2 >= r.start && kv._2 <= r.last).toLong)) ::: stats

    // add the list of user defined category appearance thresholds so that we can count the number of categories that are under them.
    stats = $(underThresholds).map(t => ("<" + t) -> ((m: Map[Any, Long]) => m.count(_._2 < t).toLong)) ::: stats

    // add a filter to count the number of categories that appear only once.
    if ($(unique)) {
      stats = (unique.name -> ((m: Map[Any, Long]) => m.count(_._2 == 1L).toLong)) :: stats
    }

    // add a column to count the number of distinct categories.
    if ($(distinct)) {
      stats = (distinct.name -> ((m: Map[Any, Long]) => m.size.toLong)) :: stats
    }

    // add a column to count the non-null rows.
    if ($(count)) {
      stats = (count.name -> ((m: Map[Any, Long]) => m.values.sum)) :: stats
    }

    stats
  }

  protected def validateInputType(input: StructField): Unit = {
    require(input.dataType == StringType, "Input type must be string type")
  }

  def this() = this(Identifiable.randomUID("catStats"))

  protected final def summaryFileName = uid + summaryFileSuffix + ".csv"

  protected def writeDistinctCounts(folder: String, column: String, list: List[(Any, Long)]): Unit = {
    val writer = new CsvWriter(folder + column + ".csv", "UTF-8", false)

    list.sortBy { case (key, value) => (-value, key.toString) }.foreach { case (key, value) =>
      writer.write(List(key.toString, value.toString))
    }

    writer.close()
  }

  protected def writeSummary(folder: String, schema: StructType, rows: Seq[Row]): Unit = {
    val writer = new CsvWriter(folder + summaryFileName, "UTF-8", false)
    writer.write(schema.fieldNames.toSeq)
    writer.writeAll(rows.sortBy(r => r.getString(0)).map(r => r.toSeq.map(Option(_).map(_.toString).getOrElse(""))))
    writer.close()
  }

  override def transformSchema(schema: StructType): StructType = {
    schema.filter(field => isDefined(inputCols) && $(inputCols).contains(field.name)).foreach(field => validateInputType(field))
    new StructType(StructField("columns", StringType) +: buildUserDefinedStats.map(s => StructField(s._1, LongType, false)).toArray)
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = transformSchema(dataset.schema)
    val sqlContext = dataset.sqlContext

    val filteredCols = if (!isDefined(inputCols) || $(inputCols).isEmpty) {
      dataset.schema.filter(f => f.dataType == StringType).map(_.name)
    } else {
      $(inputCols).toList
    }

    val summary = dataset.toDF.countDistinct(filteredCols.head, filteredCols.tail: _*)
    val aggregates = summary.map(s => buildUserDefinedStats.map { case (_, agg) => agg(s) })
    val rows = sqlContext.sparkContext.makeRDD(filteredCols.zip(aggregates).map { case (name, aggs) => Row.fromSeq(name +: aggs) })

    if (isDefined(debugPath)) {
      val folder = $(debugPath) + uid + "/"
      filteredCols.zip(summary).foreach { case (col, sm) => writeDistinctCounts(folder, col, sm.toList) }
      writeSummary(folder, schema, rows.collect())
    }

    sqlContext.createDataFrame(rows, schema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

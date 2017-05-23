package com.szadowsz.ulster.spark.transformers.string

import com.szadowsz.common.io.write.CsvWriter
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory

/**
  * Created on 06/12/2016.
  */
class StringMappingGenerator(override val uid: String) extends Transformer {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")
  protected val isDebug: Param[Boolean] = new Param[Boolean](this, "isDebug", "flag for outputting debugging info")

  protected val debugPath: Param[String] = new Param[String](this, "debugPath", "filepath for debug info")

  protected val inputCols: Param[Array[String]] = new Param[Array[String]](this, "inputCols", "input Columns", ParamValidators.arrayLengthGt[String](0))

  protected def write(folder: String, rows: Seq[Row]): Unit = {
    val writer = new CsvWriter(s"${folder}mapping.csv", "UTF-8", false)
    writer.writeAll(rows.map(r => r.toSeq.map(Option(_).map(_.toString).getOrElse(""))).sortBy(r => r.head).distinct)
    writer.close()
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}",uid)
    val result = dataset.select($(inputCols).head, $(inputCols).tail: _*)
    if ($(isDebug)) write($(debugPath),result.collect())
    result
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = StructType(schema.filter(f => $(inputCols).contains(f.name)))
}

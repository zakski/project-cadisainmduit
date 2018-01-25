package com.szadowsz.cadisainmduit.ships.wiki.us

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature.{CsvColumnExtractor, RegexGroupExtractor, StringFiller, StringMapper, StringStatistics, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 27/04/2016.
  */
object UsNavyPreparer {
  private val _logger = LoggerFactory.getLogger(UsNavyPreparer.getClass)

  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, lines: Array[String]): (Array[String], RDD[String]) = {
    if (dropFirst) {
      // assume the first line is the header
      val schema = new CsvListReader(new StringReader(lines.head.asInstanceOf[String]), CsvPreference.STANDARD_PREFERENCE).read().asScala.toArray
      (schema, sess.sparkContext.parallelize(lines.drop(1)))
    } else {
      (Array(), sess.sparkContext.parallelize(lines))
    }
  }

  protected def extractFile(sess: SparkSession, f: File, dropFirst: Boolean, readSchema: Boolean = false): DataFrame = {
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.map(_.toString)

    val (fields, stringRdd) = convertToRDD(sess, dropFirst, lines)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    var df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))

    if (readSchema && dropFirst) {
      val t = new CsvColumnExtractor("X").setInputCol("fields").setOutputCols(fields).setSize(fields.length)
      df = t.transform(df)
    }
    df.cache() // cache the constructed dataframe
    df
  }

  private def buildShipPipe(): Lineage = {
    val pipe = new Lineage("ship")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> UsNavySchema.classSchema, "size" -> UsNavySchema.classSchema.length)
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(?:(?:USNS|USS) ){0,1}(.*)$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(.*?) \\(.*$")
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "name", "pattern" -> "^\\D+$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "desc", "pattern" -> UsNavySchema.linePat)
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "descStart", "pattern" -> "^.*?([\\d]{4}).*$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "descEnd", "pattern" -> "^.*?(?:[\\d]{4}).*([\\d]{4}).*?$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "fate", "pattern" -> UsNavySchema.datePat)
    pipe.addStage(classOf[StringFiller], "outputCol" -> "navy", "value" -> "USN")
    pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "Usa")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/us/"))
    pipe
  }

  private def buildClassAndTypePipe(): Lineage = {
    val rateData = new CsvReader("./archives/dict/ships/rateMap.csv")
    val rateMap = rateData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val typeData = new CsvReader("./archives/dict/ships/classAndTypeMap.csv")
    val typeMap = typeData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val pipe = new Lineage("candt")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "classAndTypeDesc")
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "classAndTypeDesc", "delimiters" -> List(' ', '-'))
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "pattern" -> "^(?:[\\d]{1,3}[ -]Gun ){0,1}(.*)$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "outputCol" -> "classDesc", "pattern" -> "^(.*?)[ -]Class.*$")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "classDesc")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "pattern" -> "^(?:.*?[ -]Class){0,1}(.*)$")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "classAndTypeDesc")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "outputCol" -> "rateDesc", "pattern" -> "^(.*?[ -]Rate).*$")
    pipe.addStage(classOf[StringMapper], Map("mapping" -> rateMap, "inputCol" -> "rateDesc"))
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "pattern" -> "^(?:.*?[ -]Rate){0,1}(.*)$")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "classAndTypeDesc")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "outputCol" -> "typeDesc", "pattern" -> "^(Type \\d\\d).*$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "pattern" -> "^(?:Type \\d\\d ){0,1}(.*)$")
    pipe.addStage(classOf[StringMapper], Map("mapping" -> typeMap, "inputCol" -> "classAndTypeDesc"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/us/"))
    pipe
  }

  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val dfShip = extractFile(sess, new File("./data/data/web/shipsUSA.csv"), false)

    val sPipe = buildShipPipe()
    val rSpModel = sPipe.fit(dfShip)
    val rdfShip = UsNavySchema.shapeData(rSpModel.transform(dfShip))

    val candtPipe = buildClassAndTypePipe()
    val candtModel = candtPipe.fit(rdfShip)
    val candtDF = UsNavySchema.shapeFinalData(candtModel.transform(rdfShip))

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(candtDF, "./data/tmp/usn/usnInfo.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}
package com.szadowsz.cadisainmduit.ships.uboat

import java.io.{File, StringReader}
import java.sql.Date
import java.text.SimpleDateFormat

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringMapper, StringTrimmer}
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColFilterTransformer, ColRenamerTransformer, CsvTransformer}
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Created on 27/04/2016.
  */
object UboatPreparer {
  private val _logger = LoggerFactory.getLogger(UboatPreparer.getClass)

  val dateUdf = udf[Date, String]((s: String) => {
    val sDf = new SimpleDateFormat("dd MMM yyyy")
    val sDf2 = new SimpleDateFormat("MMM yyyy")
    val sDf3 = new SimpleDateFormat("yyyy")
    Try(sDf.parse(s)).orElse(Try(sDf2.parse(s))).orElse(Try(sDf3.parse(s))).map(d => new Date(d.getTime)).toOption.orNull
  })

  val dateComboUdf = udf[Date, Date,Date]((d1: Date,d2 : Date) => if (d1 == null) d2 else d1)

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
      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(fields).setSize(fields.length)
      df = t.transform(df)
    }
    df.cache() // cache the constructed dataframe
    df
  }

  private def buildClassPipe(): Lineage = {
    val pipe = new Lineage("UboatClass")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> UboatSchema.classSchema, "size" -> UboatSchema.classSchema.length)
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "built", "outputDataType" -> IntegerType)
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "planned", "outputDataType" -> IntegerType)
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/uboat/"))
    pipe
  }

  private def buildShipPipe(): Lineage = {
    val pipe = new Lineage("UboatShip")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> UboatSchema.shipSchema, "size" -> UboatSchema.shipSchema.length)
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/uboat/"))
    pipe
  }

  private def buildInfoPipe(): Lineage = {
    val pipe = new Lineage("UboatInfo")
    pipe.addStage(classOf[StringMapper], "inputCol" -> "navy", "outputCol" -> "country", "mapping" -> UboatSchema.alliesMap)
    pipe.addStage(classOf[StringMapper], "inputCol" -> "navy", "mapping" -> UboatSchema.navyMap)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "type")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "type")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "class")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "commissioned")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "endService")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(.+?)( \\(.+?\\)){0,1}$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(?:[HU][A-Z]{1,3}[SC] ){0,1}(.+?)$")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^\\D+$")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^((?!HMS).)*$")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^((?!USS).)*$")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/uboat/"))
    pipe
  }

  private def buildExtPipe(): Lineage = {
    val pipe = new Lineage("UboatExt")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/uboat/"))
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

    DeleteUtil.delete(new File("./data/web/uboat"))
    ZipperUtil.unzip("./archives/data/web/uboat.zip", "./data/web/uboat")
    val dfShip = extractFile(sess, new File("./data/web/uboat/uboatShip.csv"), false)
    val dfClass = extractFile(sess, new File("./data/web/uboat/uboatClass.csv"), false)

    val cPipe = buildClassPipe()
    val rClModel = cPipe.fit(dfClass)
    val rdfClass = rClModel.transform(dfClass)

    val sPipe = buildShipPipe()
    val rSpModel = sPipe.fit(dfShip)
    val rdfShip = rSpModel.transform(dfShip)

    val rdf = rdfShip.join(rdfClass, Seq("classUrl"), "outer")

    val iPipe = buildInfoPipe()
    val rInModel = iPipe.fit(rdf)
    val rInf = rInModel.transform(rdf)
    val serviceUDF = udf[Boolean, String]((s: String) => s != null && s.length > 0)
    val rInfTmp = rInf.select("name", "type", "class", "navy", "country", "commissioned", "endService", "lost")
      .filter(col("country") =!= "Other")
      .filter(col("class") =!= "[No specific class]")
      //.withColumn("served", serviceUDF(col("commissioned")))
      .withColumn("startDate", dateUdf(col("commissioned")))
      .withColumn("endServiceDate", dateUdf(col("endService")))
      .withColumn("lossDate", dateUdf(col("lost")))
      .withColumn("endDate", dateComboUdf(col("endServiceDate"),col("lossDate")))
      .withColumn("daysActive", datediff(col("endDate"),col("startDate")))
      .drop("commissioned", "endService", "lost", "endServiceDate","lossDate")

    val ePipe = buildExtPipe()
    val rEModel = ePipe.fit(rInfTmp)
    val rInfx = rEModel.transform(rInfTmp)

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(rInfx, "./data/web/uboat/uboatInfo.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}
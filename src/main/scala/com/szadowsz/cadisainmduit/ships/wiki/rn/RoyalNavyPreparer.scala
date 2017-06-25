package com.szadowsz.cadisainmduit.ships.wiki.rn

import java.io.{File, StringReader}

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller, StringMapper, StringTrimmer}
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
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
object RoyalNavyPreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(RoyalNavyPreparer.getClass)

  private def buildShipPipe(): Lineage = {
    val pipe = new Lineage("ship")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> RoyalNavySchema.classSchema, "size" -> RoyalNavySchema.classSchema.length)
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^\\D+$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "desc", "pattern" -> RoyalNavySchema.linePat)
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "descStart", "pattern" -> "^.*?([\\d]{4}).*$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "line_desc", "outputCol" -> "descEnd", "pattern" -> "^.*?(?:[\\d]{4}).*([\\d]{4}).*?$")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "fate", "pattern" -> RoyalNavySchema.datePat)
    pipe.addStage(classOf[StringFiller], "outputCol" -> "navy", "value" -> "RN")
    pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "Commonwealth")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/rn/"))
    pipe
  }

  private def buildClassAndTypePipe(): Lineage = {
    val rateData = new CsvReader("./archives/dict/ships/rateMap.csv")
    val rateMap = rateData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val typeData = new CsvReader("./archives/dict/ships/classAndTypeMap.csv")
    val typeMap = typeData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val pipe = new Lineage("candt")
    pipe.addStage(classOf[StringTrimmer], "inputCol" -> "classAndTypeDesc")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "classAndTypeDesc", "delimiters" -> List(' ','-'))
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "classAndTypeDesc", "pattern" -> "^(?:[\\d]{4} (?:Establishment|Proposals|Amendments) ){0,1}(.*)$")
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
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/rn/"))
    pipe
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val dfShip = extractFile(sess, new File("./archives/data/web/shipsRN.csv"), false)

    val sPipe = buildShipPipe()
    val rSpModel = sPipe.fit(dfShip)
    val rdfShip = RoyalNavySchema.shapeData(rSpModel.transform(dfShip))

    val candtPipe = buildClassAndTypePipe()
    val candtModel = candtPipe.fit(rdfShip)
    val candtDF = RoyalNavySchema.shapeFinalData(candtModel.transform(rdfShip))

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(candtDF, "./data/web/rn/rnInfo.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}
package com.szadowsz.cadisainmduit.places

import java.io.{File, StringReader}
import java.sql.Date
import java.text.SimpleDateFormat

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller, StringMapper, StringTrimmer}
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColRenamerTransformer, CsvTransformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created on 27/04/2016.
  */
object NGAPreparer {
  private val _logger = LoggerFactory.getLogger(getClass)

  val ngaSchema = Array("RC", "UFI", "UNI", "LAT", "LONG", "DMS_LAT", "DMS_LONG", "MGRS", "JOG", "FC", "DSG", "PC", "CC1", "ADM1", "POP", "ELEV", "CC2", "NT", "LC", "SHORT_FORM",
    "GENERIC", "SORT_NAME_RO", "FULL_NAME_RO", "FULL_NAME_ND_RO", "SORT_NAME_RG", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NOTE", "MODIFY_DATE", "DISPLAY", "NAME_RANK",
    "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT")

  val usaSchema = Array("FEATURE_ID","FEATURE_NAME","FEATURE_CLASS","STATE_ALPHA","STATE_NUMERIC","COUNTY_NAME","COUNTY_NUMERIC","PRIMARY_LAT_DMS",
  "PRIM_LONG_DMS", "PRIM_LAT_DEC","PRIM_LONG_DEC ","SOURCE_LAT_DMS","SOURCE_LONG_DMS","SOURCE_LAT_DEC","SOURCE_LONG_DEC","ELEV_IN_M","ELEV_IN_FT","MAP_NAME",
    "DATE_CREATED", "DATE_EDITED")

  val dropList = List("UFI", "UNI", "LAT", "LONG", "MGRS", "JOG", "DSG", "PC", "ADM1", "POP", "ELEV", "CC2", "MODIFY_DATE", "SORT_NAME_RO", "DISPLAY",
    "NAME_RANK", "FULL_NAME_RO", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT","NOTE")

  val blockList = List("Botswana","Cameroon","Gambia","Ghana","Kenya","Lesotho","Malawi","Mauritius","Mozambique","Nambia","Nigeria","Rwanda","Sierra Leone",
    "South Africa", "Swaziland", "Tanzania", "Zambia", "Zimbabwe", "Bangladesh", "India", "Pakistan", "Uganda", "Sri Lanka", "Cyprus")

  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, delimiter: Char, lines: Array[String]): (Array[String], RDD[String]) = {
    // assume the first line is the header
    if (dropFirst) {
      val pref = new CsvPreference.Builder('"', delimiter, "\r\n").build
      val schema = new CsvListReader(new StringReader(lines.head.asInstanceOf[String]), pref).read().asScala.toArray
      (schema, sess.sparkContext.parallelize(lines.drop(1)))
    } else {
      (Array(), sess.sparkContext.parallelize(lines))
    }
  }

  protected def extractFile(sess: SparkSession, f: File, dropFirst: Boolean, delimiter: Char, readSchema: Boolean = false): DataFrame = {
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.map(_.toString)

    val (fields, stringRdd) = convertToRDD(sess, dropFirst, delimiter, lines)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    var df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))

    if (readSchema && dropFirst) {
      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(fields).setSize(fields.length).setDelimiter(delimiter)
      df = t.transform(df)
    }
    df.cache() // cache the constructed dataframe
    df
  }

  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false, CsvPreference.STANDARD_PREFERENCE)
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

    DeleteUtil.delete(new File("./data/places/nga"))
    val files = FileFinder.search("./archives/data/nga", Some(new ExtensionFilter(".zip", true))).filterNot(f => blockList.contains(f.getName.dropRight(4)))
    files.foreach(f => ZipperUtil.unzip(f.getAbsolutePath, "./data/places/nga"))

    val countries = FileFinder.search("./data/places/nga", Some(new ExtensionFilter(".txt", false))).filter(f => f.getName.endsWith("_populatedplaces_p.txt"))
      .map(_.getName.substring(0,2))

    val usa = extractFile(sess, new File("./data/places/nga/POP_PLACES_20170601.txt"), true, '|')

    val pipe = new Lineage("nga")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> usaSchema, "size" -> usaSchema.length, "delimiter" -> '|')
    pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "US")
    val m = pipe.fit(usa)

    val usaDF = m.transform(usa).withColumnRenamed("FEATURE_NAME","name").select("name","country")

    val otherResult = getOtherCountries(sess).select("CC1","FULL_NAME_ND_RO").withColumnRenamed("FULL_NAME_ND_RO","name").withColumnRenamed("CC1","country")

    val result = otherResult.union(usaDF).groupBy("name").agg(count("name").alias("total"),
      (countries.map(c => count(when(col("country") === c.toUpperCase,true)).alias(c)) :+ count(when(col("country") === "US",true)).alias("us")):_*)

    writeDF(result, "./data/places/places.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => s.head))
  }

  private def getOtherCountries(sess: SparkSession) = {
    val placeFiles = FileFinder.search("./data/places/nga", Some(new ExtensionFilter(".txt", false)))
      .filter(f => f.getName.endsWith("_populatedplaces_p.txt")
      )
    //   val placesDfs = placeFiles.map(f => extractFile(sess, f, true, true)).slice(0,placeFiles.length/4)
    val placesDfs = placeFiles.map(f => extractFile(sess, f, true, '\t'))

    val lcFunct = udf[Boolean, String, String]((s1: String, s2: String) => {
      (s1 == null || s1.trim.length == 0 || s1 == "eng") &&
        "[0-9]".r.findFirstMatchIn(s2).isEmpty && "^['a-z]".r.findFirstMatchIn(s2).isEmpty
    })

    val cutdownDfs = placesDfs.map { placesDf =>

      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(ngaSchema).setSize(ngaSchema.length).setDelimiter('\t')
      val csvDF = t.transform(placesDf)
      val shortDf = csvDF.drop(dropList: _*)
      val filterDf = shortDf.filter(lcFunct(col("LC"), col("FULL_NAME_ND_RO")))
      filterDf.drop("LC")
    }
    cutdownDfs.tail.foldLeft(cutdownDfs.head) { case (df1, df2) => df1.union(df2).toDF() }
  }
}
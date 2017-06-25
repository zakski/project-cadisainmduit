package com.szadowsz.cadisainmduit.places

import java.io.{File, StringReader}

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller, StringMapper, StringTrimmer}
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColRenamerTransformer, CsvTransformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 27/04/2016.
  */
object NGAPreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

  val ngaSchema = Array("RC", "UFI", "UNI", "LAT", "LONG", "DMS_LAT", "DMS_LONG", "MGRS", "JOG", "FC", "DSG", "PC", "CC1", "ADM1", "POP", "ELEV", "CC2", "NT", "LC", "SHORT_FORM",
    "GENERIC", "SORT_NAME_RO", "FULL_NAME_RO", "FULL_NAME_ND_RO", "SORT_NAME_RG", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NOTE", "MODIFY_DATE", "DISPLAY", "NAME_RANK",
    "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT")

  val usaSchema = Array("FEATURE_ID", "FEATURE_NAME", "FEATURE_CLASS", "STATE_ALPHA", "STATE_NUMERIC", "COUNTY_NAME", "COUNTY_NUMERIC", "PRIMARY_LAT_DMS",
    "PRIM_LONG_DMS", "PRIM_LAT_DEC", "PRIM_LONG_DEC ", "SOURCE_LAT_DMS", "SOURCE_LONG_DMS", "SOURCE_LAT_DEC", "SOURCE_LONG_DEC", "ELEV_IN_M", "ELEV_IN_FT", "MAP_NAME",
    "DATE_CREATED", "DATE_EDITED")

  val dropList = List("UFI", "UNI", "LAT", "LONG", "MGRS", "JOG", "DSG", "PC", "ADM1", "POP", "ELEV", "CC2", "MODIFY_DATE", "SORT_NAME_RO", "DISPLAY",
    "NAME_RANK", "FULL_NAME_RO", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT", "NOTE")

  val blockList = List("Botswana", "Cameroon", "Gambia", "Ghana", "Kenya", "Lesotho", "Malawi", "Mauritius", "Mozambique", "Nambia", "Nigeria", "Rwanda", "Sierra Leone",
    "South Africa", "Swaziland", "Tanzania", "Zambia", "Zimbabwe", "Bangladesh", "India", "Pakistan", "Uganda", "Sri Lanka", "Cyprus", "Malaysia")

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    DeleteUtil.delete(new File("./data/places/nga"))
    val files = FileFinder.search("./archives/data/nga", Some(new ExtensionFilter(".zip", true))).filterNot(f => blockList.contains(f.getName.dropRight(4)))
    files.foreach(f => ZipperUtil.unzip(f.getAbsolutePath, "./data/places/nga"))

    val countries = FileFinder.search("./data/places/nga", Some(new ExtensionFilter(".txt", false))).filter(f => f.getName.endsWith("_populatedplaces_p.txt"))
      .map(_.getName.substring(0, 2))

    val usa = extractFile(sess, new File("./data/places/nga/POP_PLACES_20170601.txt"), true, false,'|')

    val usaResult: DataFrame = getUSA(usa)
    val otherResult = getOtherCountries(sess)
    val unitedResult = otherResult.union(usaResult)

    val filResult: DataFrame = getFilteredData(unitedResult)

    val result = filResult.groupBy("name").agg(count("name").alias("total"),
      (countries.map(c => count(when(col("country") === c.toUpperCase, true)).alias(c)) :+ count(when(col("country") === "US", true)).alias("us")): _*)

    val sums = result.columns.filterNot(_ == "name").map(c => sum(col(c)).alias(c))
    val gramResult = groupUp(buildGrammerProbs(result))

    writeDF(result, "./data/places/places.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
    writeDF(gramResult, "./data/places/fullGrammar.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
  }

  private def groupUp(df: DataFrame): DataFrame = {
    val sums = df.columns.filterNot(_ == "name").map(c => sum(col(c)).alias(c))
    df.groupBy("name").agg(sums.head, sums.tail: _*)
  }

  private def getFilteredData(unitedResult: Dataset[Row]) = {
    //    val goodName = udf[Boolean,String]((name : String) =>
    //      !(
    //        name.contains("Mobile Home Park") ||
    //          name.contains("Mobile Estates") ||
    //          name.contains("Mobile Manor") ||
    //          name.contains("Mobile Village") ||
    //          name.contains("Mobile Home Village")
    //        )
    //    )
    val pipe = new Lineage("nga")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(.+?)( \\(historical\\)){0,1}$")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^\\D+$")
    val m = pipe.fit(unitedResult)
    val filResult = m.transform(unitedResult)
    filResult //.filter(goodName(col("name")))
  }

  private def buildGrammerProbs(df: DataFrame): DataFrame = {
    val (g,f) = PlaceGrammar.buildGrammar(df)
    writeDF(groupUp(g), "./data/places/grammar.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
    f
  }

  private def getUSA(usa: DataFrame) = {
    val pipe = new Lineage("nga-usa")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> usaSchema, "size" -> usaSchema.length, "delimiter" -> '|')
    pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "US")
    val m = pipe.fit(usa)

    val usaResult = m.transform(usa).withColumnRenamed("FEATURE_NAME", "name").select("name", "country")
    usaResult
  }

  private def getOtherCountries(sess: SparkSession) = {
    val placeFiles = FileFinder.search("./data/places/nga", Some(new ExtensionFilter(".txt", false)))
      .filter(f => f.getName.endsWith("_populatedplaces_p.txt")
      )
    //   val placesDfs = placeFiles.map(f => extractFile(sess, f, true, true)).slice(0,placeFiles.length/4)
    val placesDfs = placeFiles.map(f => extractFile(sess, f, true, false,'\t'))

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
      .withColumnRenamed("FULL_NAME_ND_RO", "name").withColumnRenamed("CC1", "country").select("name", "country")
  }
}
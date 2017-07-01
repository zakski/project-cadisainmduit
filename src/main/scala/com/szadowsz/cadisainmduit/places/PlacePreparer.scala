package com.szadowsz.cadisainmduit.places

import java.io.File

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.spelling.RegexValidationTransformer
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created on 27/04/2016.
  */
object PlacePreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

  val blockList = List("Botswana", "Cameroon", "Gambia", "Ghana", "Kenya", "Lesotho", "Malawi", "Mauritius", "Mozambique", "Nambia", "Nigeria", "Rwanda",
    "Sierra Leone", "South Africa", "Swaziland", "Tanzania", "Zambia", "Zimbabwe", "Bangladesh", "India", "Pakistan", "Uganda", "Sri Lanka", "Cyprus",
    "Malaysia", "United Kingdom")

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


    val ukResult = OGPlacePreparer.getUK(sess)

    val usaResult: DataFrame = USAPreparer.getUSA(sess)
    val otherResult = NGAPreparer.getOtherCountries(sess)
    val unitedResult = otherResult.union(usaResult).union(ukResult)

    val filResult: DataFrame = getFilteredData(unitedResult)

    val result = filResult.groupBy("name").agg(count("name").alias("total"),
      (countries.map(c => count(when(col("country") === c.toUpperCase, true)).alias(c)) :+
        count(when(col("country") === "US", true)).alias("us")  :+  count(when(col("country") === "uk", true)).alias("uk")
        ): _*)

    val sums = result.columns.filterNot(_ == "name").map(c => sum(col(c)).alias(c))
    val gramResult = groupUp(buildGrammerProbs(result))

    writeDF(result, "./data/places/places.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
    writeDF(gramResult, "./data/places/fullGrammar.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (/*-s(1).toInt,*/ s.head)))
  }

  private def groupUp(df: DataFrame): DataFrame = {
    val sums = df.columns.filterNot(_ == "name").map(c => sum(col(c)).alias(c))
    df.groupBy("name").agg(sums.head, sums.tail: _*)
  }

  private def getFilteredData(unitedResult: Dataset[Row]) = {
    val pipe = new Lineage("nga")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(.+?)( \\(historical\\)){0,1}$")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "^\\D+$")
    val m = pipe.fit(unitedResult)
    val filResult = m.transform(unitedResult)
    filResult
  }

  private def buildGrammerProbs(df: DataFrame): DataFrame = {
    val (g, f) = PlaceGrammar.buildGrammar(df)
    writeDF(g, "./data/places/grammar.csv", "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
    f
  }
}
package com.szadowsz.cadisainmduit.places.nga.commonwealth

import java.io.File

import com.szadowsz.cadisainmduit.places.nga.NGASchema
import com.szadowsz.spark.ml.feature.{CsvColumnExtractor, RegexGroupExtractor, RegexValidator}
import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

trait BaseNGACommonwealthPreparer extends LocalDataframeIO {
  protected val _logger: Logger = LoggerFactory.getLogger(getClass)

  protected def transform(dataPath : String, resultPath : String, customFilter : (File) => Boolean): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val (commonwealthCodes: Array[String], commonwealthDF: DataFrame) = loadData(dataPath, customFilter, sess)

    val filResult: DataFrame = getFilteredData(commonwealthDF)

    val result = filResult.groupBy("name").agg(count("name").alias("total"),
      commonwealthCodes.map(c => count(when(upper(col("country")) === c.toUpperCase, true)).alias(c)): _*)

    val sums = result.columns.filterNot(_ == "name").map(c => sum(col(c)).alias(c))
    val f = udf[Boolean,String](n => n.contains(" "))

    writeDF(result, resultPath, "UTF-8", (row: Seq[String]) => true, Ordering.by((s: Seq[String]) => (-s(1).toInt, s.head)))
  }

  protected def loadData(dataPath: String, customFilter: File => Boolean, sess: SparkSession): (Array[String], DataFrame) = {
    val commonwealthCodes = Commonwealth.gerCountryCodes(dataPath, customFilter)
    val commonwealthDF = getNGACommonwealthDf(sess, dataPath, customFilter)
    (commonwealthCodes, commonwealthDF)
  }

  private def getNGACommonwealthDf(sess: SparkSession, dataPath : String, customFilter : (File) => Boolean) = {
    val commonwealthFiles = Commonwealth.getCountryFiles(dataPath, sess,customFilter)

    val lcFunct = udf[Boolean, String, String]((s1: String, s2: String) => {
      (s1 == null || s1.trim.length == 0 || s1 == "eng") &&
        "[0-9]".r.findFirstMatchIn(s2).isEmpty && "^['a-z]".r.findFirstMatchIn(s2).isEmpty
    })

    val cutdownDfs = commonwealthFiles.map{f =>
      val placesDf = extractFile(sess, f, true, false, '\t')

      val t = new CsvColumnExtractor("X").setInputCol("fields").setOutputCols(NGASchema.columns).setSize(NGASchema.columns.length).setDelimiter('\t')
      val csvDF = t.transform(placesDf)
      val shortDf = csvDF.drop(NGASchema.dropList: _*)
      val filterDf = shortDf.filter(lcFunct(col("LC"), col("FULL_NAME_ND_RO")))
      filterDf.drop("LC")
    }
    cutdownDfs.tail.foldLeft(cutdownDfs.head) { case (df1, df2) => df1.union(df2).toDF() }
      .withColumnRenamed("FULL_NAME_ND_RO", "name").withColumnRenamed("CC1", "country").select("name", "country")
  }

  private def getFilteredData(unitedResult: Dataset[Row]) = {
    val pipe = new Lineage("nga")
    pipe.addStage(classOf[RegexGroupExtractor], "inputCol" -> "name", "pattern" -> "^(.+?)( \\(historical\\)){0,1}$")
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "name", "pattern" -> "^\\D+$")
    val m = pipe.fit(unitedResult)
    val filResult = m.transform(unitedResult)
    filResult
  }
}

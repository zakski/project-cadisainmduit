package com.szadowsz.cadisainmduit.census.usa

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.zip.ZipperUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaCensusYearSplicer extends CensusHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def extractYearFile(sess: SparkSession, f: File): (String, DataFrame) = {
    val year = f.getName.substring(3, 7) // assume the year is the first 4 characters of the file name
    val r = new FReader(f.getAbsolutePath)
    val stringRdd = sess.sparkContext.parallelize(r.lines().toArray)
    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache() // cache the constructed dataframe
    (year, df)
  }

  protected def buildCols(country: String, year: String): Array[String] = {
    Array("name", "gender", s"${country}_count_$year")
  }

  protected def spliceYearData(sess: SparkSession, path: String, country: String): DataFrame = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".txt", false)))
    val dfs = files.map { f =>
      val (year, df) = extractYearFile(sess, f)
      df.cache()

      val pipe: Lineage = buildStdPipeline(s"$year-$country-caps", buildCols(country, year), None)

      val model = pipe.fit(df)
      val tmp = model.transform(df)
      selectYearCols(country, year, tmp).dropDuplicates(Array("name","gender"))
    }
    val all = dfs.tail.foldLeft(dfs.head) { case (comp, curr) =>
      val res = comp.join(curr, Seq("name", "gender"), "outer")
        res.cache()
        res
    }
    all
  }

  protected def selectYearCols(country: String, year: String, tmp: DataFrame): DataFrame = {
    tmp.select("name", "gender", s"${country}_count_$year")
  }

  def loadData(save: Boolean): DataFrame = {
    DeleteUtil.delete(new File("./data/census/us/namesByYear/"))
    ZipperUtil.unzip(s"./archives/data/census/us/names.zip","./data/census/us/namesByYear")

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val yearData: DataFrame = spliceYearData(sess, "./data/census/us/namesByYear", "US")

    if (yearData.count() < 1) {
      DeleteUtil.delete(new File("./data/census/us/names/"))
      throw new RuntimeException(yearData.schema.fieldNames.mkString("|"))
    }


    val popFields = yearData.schema.fieldNames.filter(f => f.contains("count"))
    val pipe = buildFractionPipeline("US-frac","US",popFields,popFields)
    val m = pipe.fit(yearData)
    val all = m.transform(yearData)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
      writeDF(all,"./data/census/us/babynames_USA.csv","UTF-8",(s : Seq[String])=> true,ord)
    }

    DeleteUtil.delete(new File("./data/census/us/namesByYear/"))
     yearData
  }

    def main(args: Array[String]): Unit = {
    loadData(true)
    }
//  ZipperUtil.unzip("./archives/census/us/names.zip", "./data/census/us/names/")
//      ZipperUtil.unzip("./archives/census/us/namesByState.zip", "./data/census/us/namesByState/")
}


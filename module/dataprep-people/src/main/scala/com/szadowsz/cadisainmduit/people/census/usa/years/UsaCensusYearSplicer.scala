package com.szadowsz.cadisainmduit.people.census.usa.years

import java.io.File

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.spark.ml.Lineage
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
trait UsaCensusYearSplicer extends CensusHandler {
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

  protected def spliceYearData(sess: SparkSession, path: String, country: String, sYear : Int, eYear: Int): DataFrame = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".txt", false))).filter{ f =>
      val year = f.getName.substring(3, 7).toInt
      year >= sYear && year < eYear
    }
    val dfs = files.map { f =>
      val (year, df) = extractYearFile(sess, f)
      df.cache()

      val pipe: Lineage = buildStdPipeline(s"$year-$country-caps", buildCols(country, year))

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

  def loadData(save: Boolean, sYear : Int, eYear: Int): DataFrame = {
   val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val yearData: DataFrame = spliceYearData(sess, "./data/data/census/us/names", "US",sYear,eYear)

    if (yearData.count() < 1) {
      throw new RuntimeException(yearData.schema.fieldNames.mkString("|"))
    }

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
      writeDF(yearData,s"./data/tmp/USA/baby_names_${sYear}_until_$eYear.csv","UTF-8",(s : Seq[String])=> true,ord)
    }

   yearData
  }
}


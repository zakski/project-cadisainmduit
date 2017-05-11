package com.szadowsz.cadisainmduit.census.usa.regions

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.math.NullTransformer
import com.szadowsz.ulster.spark.transformers.{CastTransformer, CsvTransformer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, _}
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaRegionsNameSplicer extends CensusHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def extractYearFile(sess: SparkSession, f: File): (String, DataFrame) = {
    val reg = f.getName.substring("babynames_".length, f.getName.lastIndexOf(".csv"))
    val r = new FReader(f.getAbsolutePath)
    val stringRdd = sess.sparkContext.parallelize(r.lines().toArray.drop(1))
    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache() // cache the constructed dataframe
    (reg, df)
  }

  protected def spliceYearData(sess: SparkSession, path: String): DataFrame = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".csv", false)))
    val dfs = files.map { f =>
      val (reg, df) = extractYearFile(sess, f)
      df.cache()

      val pipe = new Lineage(reg)
      val cols = Array("name", "gender", s"${reg}_appfrac", s"${reg}_avgVal")
      pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
      pipe.addStage(classOf[CastTransformer], "inputCol" -> s"${reg}_appfrac", "outputDataType" -> DoubleType)
      pipe.addStage(classOf[CastTransformer], "inputCol" -> s"${reg}_avgVal", "outputDataType" -> DoubleType)

      val model = pipe.fit(df)
      model.transform(df)
    }
    val all = dfs.tail.foldLeft(dfs.head) { case (comp, curr) =>
      val res = comp.join(curr, Seq("name", "gender"), "outer")
      res.cache()
      res
    }
    all
  }

  def loadData(save: Boolean): DataFrame = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val yearData: DataFrame = spliceYearData(sess, "./data/census/us/")

    if (yearData.count() < 1) {
      DeleteUtil.delete(new File("./data/census/us/names/"))
      throw new RuntimeException(yearData.schema.fieldNames.mkString("|"))
    }


    val pipe = new Lineage("cleanup")
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)

    val m = pipe.fit(yearData)
    val all = m.transform(yearData)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
      writeDF(all, "./data/census/us_baby_names.csv", "UTF-8", (s: Seq[String]) => true, ord)
    }
    yearData
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }

  //  ZipperUtil.unzip("./archives/census/us/names.zip", "./data/census/us/names/")
  //      ZipperUtil.unzip("./archives/census/us/namesByState.zip", "./data/census/us/namesByState/")
}


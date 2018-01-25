package com.szadowsz.cadisainmduit.people.census

import java.io.File

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.spark.ml.Lineage
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
  * Created on 13/01/2017.
  */
trait SegGenderCensusHandler extends CensusHandler {
  protected val logger = LoggerFactory.getLogger(this.getClass)

  protected def extractFile(sess: SparkSession, f: File): (String, DataFrame) = {
    val year = f.getName.substring(0, 4) // assume the year is the first 4 characters of the file name
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.drop(1) // assume the first line is the header
    val stringRdd = sess.sparkContext.parallelize(lines)
    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    (year, df)
  }

  /**
    * Method to get the initial columns
    *
    * @param region the region being used
    * @param year   the year of the data
    * @return array of columns
    */
  protected def getInitialCols(region: String, year: String): Array[String] = {
    Array("name", s"${region}_count_$year", s"${region}_rank_$year")
  }


  protected def selectStdCols(region: String, year: String, tmp: DataFrame): DataFrame = {
    tmp.select("name", s"${region}_count_$year")
  }

  /**
    * Method to combine individual mono-gender year datasets into one big mono-gender set
    *
    * @param sess   the spark sesssion
    * @param path   local root folder for the datasets
    * @param region the region we are processing for
    * @param gender the gender we are processing for
    * @return the combined mono-gender dataset
    */
  protected def spliceYearData(sess: SparkSession, path: String, region: String, gender: Char): DataFrame = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".csv", false))) // get a list of data files
    val result = files.tail.foldLeft(loadYearData(sess,region,files.head)) { case (df,f) =>
      df.join(loadYearData(sess, region, f), Seq("name"), "outer")
    }.withColumn("gender",lit(gender.toString))
    result.select("name","gender" +: result.columns.tail.dropRight(1):_*)
  }

  protected def loadYearData(sess: SparkSession, region: String, f: File): Dataset[Row] = {
    val (year, df) = extractFile(sess, f)

    // process each individual datatset using the standard personal name preparation pipeline
    val pipe: Lineage = buildStdPipeline(s"$year-$region-caps", getInitialCols(region, year))

    val (model,tmp) = pipe.fitAndTransform(df)
    selectStdCols(region, year, tmp).dropDuplicates(Array("name"))
  }

  protected def aggData(region: String, boys: DataFrame, girls: DataFrame): DataFrame = {
    val all = boys.union(girls)
    val appFields = all.schema.fieldNames.filter(f => f.contains("count") || f.contains("rank"))
    val popFields = all.schema.fieldNames.filter(f => f.contains("count"))

    val pipe: Lineage = buildFractionPipeline(s"$region-frac", region, appFields, popFields)
    val m = pipe.fit(all)
    m.transform(all)
  }

  protected def loadBoysData(region: String, rootBoys: String, sess: SparkSession) = {
    val boys = spliceYearData(sess, s"$rootBoys/boys/", region, 'M')

    if (boys.count() < 1) {
      throw new RuntimeException(boys.schema.fieldNames.mkString("|"))
    }
    boys
  }

  protected def loadGirlsData(region: String, girlRoot: String, sess: SparkSession) = {
    val girls = spliceYearData(sess, s"$girlRoot/girls/", region, 'F')

    if (girls.count() < 1) {
      throw new RuntimeException(girls.schema.fieldNames.mkString("|"))
    }
    girls
  }

  /**
    * Public method to load in the segregated dataset
    *
    * @param save whether or not to dump the prepared data into a temp file.
    * @return the prepared dataset.
    */
  def loadData(save: Boolean): DataFrame


  /**
    * Private Method to load the segregated gender census files
    *
    * @param save      whether or not to dump the prepared data into a temp file.
    * @param region    the region that is being loaded
    * @param rootBoys  the boy specific file root
    * @param rootGirls the girl specific file root
    * @return the prepared dataset.
    */
  protected def loadData(save: Boolean, region: String, rootBoys: String, rootGirls: String = null): DataFrame = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    // Try to load the boys first, combining all year files into a single dataset
    val boys: DataFrame = loadBoysData(region, rootBoys, sess)

    // Then Try the Girls, combining all year files into a single dataset
    val girlRoot = Option(rootGirls).getOrElse(rootBoys)
    val girls: DataFrame = loadGirlsData(region, girlRoot, sess)

    // Finally merge the boy and girls dataset.
    val children = boys.union(girls)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
      writeDF(children, s"./data/tmp/$region/baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }
    children
  }
}

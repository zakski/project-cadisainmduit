package com.szadowsz.naming.people.prep

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
trait CensusFileCombiner extends CensusHandler {
  protected val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Method to get the initial columns
    *
    * @param region the region being used
    * @param year   the year of the data
    * @return array of columns
    */
  protected def getInitialCols(headers : Array[String],region: String, year: String): Array[String] = {
    headers.head +: headers.tail.map(_ + "_" + region + "_" +year)
  }
  
  protected def selectCols(region: String, year: String, schema: Array[String], tmp: DataFrame): Dataset[Row]
  
  
  protected def extractFile(sess: SparkSession, f: File): (String, Array[String], DataFrame) = {
    val year = "[0-9]{4}".r.findFirstIn(f.getName).get // assume the year is only 4 number sequence in name
    val schema = extractSchema(f)
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.drop(1) // assume the first line is the header
    val stringRdd = sess.sparkContext.parallelize(lines)
    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    (year, schema,df)
  }
  
  
  protected def loadYearData(sess: SparkSession, region: String, f: File): Dataset[Row] = {
    val (year, rawSchema, df) = extractFile(sess, f)
    
    val schema = getInitialCols(rawSchema,region, year)
    
    // process each individual datatset using the standard personal name preparation pipeline
    val pipe: Lineage = buildStdPipeline(s"$year-$region-caps", schema)
    
    val (model,tmp) = pipe.fitAndTransform(df)
    selectCols(region, year, schema,tmp)
  }
  
  /**
    * Method to combine individual year datasets into one big set
    *
    * @param sess   the spark sesssion
    * @param path   local root folder for the datasets
    * @param region the region we are processing for
    * @return the combined mono-gender dataset
    */
  protected def spliceYearData(sess: SparkSession, path: String, region: String): DataFrame = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".csv", false))) // get a list of data files
    val result = files.tail.foldLeft(loadYearData(sess,region,files.head)) { case (df,f) => 
      df.join(loadYearData(sess, region, f), Seq("name"), "outer")
    }
    result.select("name",result.columns.tail.dropRight(1):_*)
    //loadYearData(sess,region,files.head)
  }
  
  /**
    * combine all yearly datasets into one dataset
    * @param region the region we are processing for
    * @param root   local root folder for the datasets
    * @param sess   the spark sesssion
    * @return the combined dataset
    */
  protected def loadFilesData(region: String, root: String, sess: SparkSession): DataFrame = {
    val names = spliceYearData(sess, s"$root/", region)

    if (names.count() < 1) {
      throw new RuntimeException(names.schema.fieldNames.mkString("|"))
    }
    names
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
    * @param root  the file root
    * @return the prepared dataset.
    */
  protected def loadAndCombineData(save: Boolean, name : String, region: String, root: String): DataFrame = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    // Try to load the boys first, combining all year files into a single dataset
    val df: DataFrame = loadFilesData(region, root, sess)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
      writeDF(df, s"./data/tmp/$region/$name.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }
    df
  }
}

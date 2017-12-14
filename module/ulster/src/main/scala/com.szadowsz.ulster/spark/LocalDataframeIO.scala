package com.szadowsz.ulster.spark

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 21/06/2017.
  */
trait LocalDataframeIO {

  /**
    * General Purpose CSV file to RDD[String] Reader.
    *
    * @param sess the spark session
    * @param dropFirst if the csv file has any headers
    * @param delimiter the deliminator of the the fields of the csv (project has source data with tabs)
    * @param lines the lines extracted from the file by the file reader (separate to the conversion to spark to allow for the dropping of the header)
    * @return a tuple of the headers and the resulting RDD
    */
  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, delimiter: Char, lines: Array[String]): (Array[String], RDD[String]) = {
    if (dropFirst)  {
    val pref = new CsvPreference.Builder('"', delimiter, "\r\n").build
      val schema = new CsvListReader(new StringReader(lines.head.asInstanceOf[String]), pref).read().asScala.toArray
      (schema, sess.sparkContext.parallelize(lines.drop(1))) // assume the first line is the header
    } else {
      (Array(), sess.sparkContext.parallelize(lines))
    }
  }

  protected def extractFile(sess: SparkSession, f: File, dropFirst: Boolean, readSchema: Boolean = false, delimiter: Char = ','): DataFrame = {
    // TODO check if this is necessary
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.map(_.toString)

    val (fields, stringRdd) = convertToRDD(sess, dropFirst, delimiter, lines)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    var df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))

    if (readSchema && dropFirst) {
      val t = new CsvTransformer(s"${f.getName} Loader").setInputCol("fields").setOutputCols(fields).setSize(fields.length).setDelimiter(delimiter)
      df = t.transform(df)
    }
    df.cache() // cache the constructed dataframe
    df
  }

  /**
    * General Purpose Dataframe to csv file writer.
    *
    * @param df
    * @param path
    * @param charset
    * @param filter
    * @param sortBy
    */
  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]], append : Boolean = false): Unit = {
    val writer = new CsvWriter(path, charset, append)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }
}

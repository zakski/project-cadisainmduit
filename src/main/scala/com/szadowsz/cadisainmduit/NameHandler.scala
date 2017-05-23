package com.szadowsz.cadisainmduit

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 15/05/2017.
  */
trait NameHandler {

  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, lines: Array[String]): (Array[String], RDD[String]) = {
    if (dropFirst) { // assume the first line is the header
    val schema = new CsvListReader(new StringReader(lines.head.asInstanceOf[String]), CsvPreference.STANDARD_PREFERENCE).read().asScala.toArray
      (schema, sess.sparkContext.parallelize(lines.drop(1)))
    } else {
      (Array(), sess.sparkContext.parallelize(lines))
    }
  }

  protected def extractFile(sess: SparkSession, f: File, dropFirst: Boolean, readSchema: Boolean = false): DataFrame = {
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.map(_.toString)

    val (fields, stringRdd) = convertToRDD(sess, dropFirst, lines)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    var df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))

    if (readSchema && dropFirst) {
      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(fields).setSize(fields.length)
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
  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }
}

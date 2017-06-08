package com.szadowsz.cadisainmduit.ships

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 05/06/2017.
  */
object ShipPreparer {

  val schema = Array("name", "type", "class", "navy", "country", "startDate", "endDate", "daysActive")

  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, lines: Array[String]): (Array[String], RDD[String]) = {
    if (dropFirst) {
      // assume the first line is the header
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

  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val dfRN = extractFile(sess, new File("./data/web/rn/rnInfo.csv"), false)
    val dfUSN = extractFile(sess, new File("./data/web/usn/usnInfo.csv"), false)
    val dfUboat = extractFile(sess, new File("./data/web/uboat/uboatInfo.csv"), false)

    val pipe = new Lineage("ship")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> schema, "size" -> schema.length)

    val model = pipe.fit(dfRN)
    val rn = model.transform(dfRN)
    val uboat = model.transform(dfUboat)
    val usa = model.transform(dfUSN)
    val ships = uboat.union(rn).union(usa).distinct()

    val pipe2 = new Lineage("ship2")
    pipe2.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/ships/"))

    val model2 = pipe2.fit(ships)
    model2.transform(ships)

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(ships, "./data/web/ships.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}

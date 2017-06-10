package com.szadowsz.cadisainmduit.ships

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.StringMapper
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import net.sf.extjwnl.data.PointerUtils
import net.sf.extjwnl.dictionary.Dictionary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Created on 05/06/2017.
  */
object ShipPreparer {

  val dictionary = Dictionary.getDefaultResourceInstance()

  val schema = Array("name", "type", "class", "navy", "country", "startDate", "endDate", "daysActive")

  def getHypernyms(s: String): Array[String] = {
    val indexes = Try(dictionary.lookupAllIndexWords(s).getIndexWordArray)
    val senses = indexes.map(_.flatMap(iw => iw.getSenses.asScala))
    val hypernyms = senses.map(_.map(s => PointerUtils.getDirectHypernyms(s)))
    val pointers = hypernyms.map(_.flatMap(list => list.iterator().asScala.toList))
    val words = pointers.map(_.flatMap(node => node.getSynset.getWords.asScala.map(_.getLemma)).distinct)
    val results = words.getOrElse(Array())
    results
  }

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

        val dfRN = extractFile(sess, new File("./data/web/rn/rnInfo.csv"), true)
        val dfUSN = extractFile(sess, new File("./data/web/usn/usnInfo.csv"), true)
        val dfUboat = extractFile(sess, new File("./data/web/uboat/uboatInfo.csv"), true)

        val pipe = new Lineage("ship")
        pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> schema, "size" -> schema.length)

        val model = pipe.fit(dfRN)
        val rn = model.transform(dfRN)
        val uboat = model.transform(dfUboat)
        val usa = model.transform(dfUSN)
        val ships = uboat.union(rn).union(usa).distinct()

        val typeData = new CsvReader("./archives/dict/ships/powerRatings.csv")
        val typeRows = typeData.readAll().drop(1)
        val ratings = typeRows.map(s => s.head.trim -> s.last.trim).toMap
        val classify = typeRows.map(s => s.head.trim -> s(1).trim).toMap

        val pipe2 = new Lineage("ship2")
        pipe2.addStage(classOf[StringMapper], Map("mapping" -> classify, "inputCol" -> "type", "outputCol" -> "role"))
        pipe2.addStage(classOf[StringMapper], Map("mapping" -> ratings, "inputCol" -> "type", "outputCol" -> "rating"))
        pipe2.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/ships/"))

        val model2 = pipe2.fit(ships)
        val idiResults = model2.transform(ships).na.fill("unknown", List("role")).withColumn("rating", col("rating").cast(DoubleType)).na.fill(12.0)

        val dictFunct = (name: String) => {
          if (!(name.contains(" ") || name.contains("-"))) {
            //dictionary.lookupAllIndexWords(name).getValidPOSSet.asScala.map(_.toString).mkString(",")
            //  Relationship.findRelationships()
            getHypernyms(name) mkString (",")
          } else {
            null
          }
        }

        val dictUDF = udf[String, String](dictFunct)

        val results = idiResults.filter(col("type") =!= "Starship").groupBy("name").agg(
          dictUDF(col("name")).alias("pos"),
          round(avg(col("rating")), 2).alias("power"),
          count(when(col("country") === "Commonwealth", true)).alias("commonwealth"),
          count(when(col("country") =!= "Commonwealth", true)).alias("usa"),
          min(col("startDate")).alias("firstUsed"),
          round(avg(col("daysActive"))).cast(IntegerType).alias("avgDaysUsed"),
          count(when(col("role") === "capital", true)).alias("capital"),
          count(when(col("role") === "battle", true)).alias("battle"),
          count(when(col("role") === "stealth", true)).alias("stealth"),
          count(when(col("role") === "bombardment", true)).alias("bombardment"),
          count(when(col("role") === "assault", true)).alias("assault"),
          count(when(col("role") === "escort", true)).alias("escort"),
          count(when(col("role") === "patrol", true)).alias("patrol"),
          count(when(col("role") === "scout", true)).alias("scout"),
          count(when(col("role") === "cargo", true)).alias("cargo"),
          count(when(col("role") === "resupply", true)).alias("resupply"),
          count(when(col("role") === "auxiliary", true)).alias("auxiliary"),
          count(when(col("role") === "unknown", true)).alias("unknown")
        )

        val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
        writeDF(results, "./data/web/ships.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}

package com.szadowsz.cadisainmduit.ships

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import com.szadowsz.spark.ml.feature.{CsvColumnExtractor, StringMapper, StringStatistics}
import net.sf.extjwnl.data.PointerUtils
import net.sf.extjwnl.dictionary.Dictionary
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._

import scala.util.Try

/**
  * Created on 05/06/2017.
  */
object ShipPreparer extends LocalDataframeIO {

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

  def getNames() = {
    val typeData = new CsvReader("./archives/results/baby_names.csv")
    val typeRows = typeData.readAll().drop(1)
    typeRows.map(s => s.head.trim).toList
  }

  def getPlaces() = {
    val typeData = new CsvReader("./archives/results/placenames.csv")
    val typeRows = typeData.readAll().drop(1)
    typeRows.map(s => s.head.trim).toList
  }

  def getOverride() = {
    val typeData = new CsvReader("./archives/dict/ships/override.txt")
    val typeRows = typeData.readAll().drop(1)
    typeRows.map(s => s.head.trim).toList
  }


  def listFunct(list : Array[String]) = {
    (name: String) => {
      val nameParts = name.split(" ")
      list.exists(v => nameParts.exists(v.toUpperCase == _.toUpperCase))
    }
  }


  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val dfRN = extractFile(sess, new File("./data/tmp/rn/rnInfo.csv"), true)
    val dfUSN = extractFile(sess, new File("./data/tmp/usn/usnInfo.csv"), true)
    val dfUboat = extractFile(sess, new File("./data/tmp/uboat/uboatInfo.csv"), true)

    val pipe = new Lineage("ship")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> schema, "size" -> schema.length)

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

    val overrides = getOverride()
    val overrideUdf = udf[Boolean,String](s => overrides.contains(s))

    val dictUDF = udf[String, String](dictFunct)

    val names = getNames()
    val nameUDF = udf[Boolean,String](listFunct(names.toArray))
    val places = getPlaces()
    val placesUDF = udf[Boolean,String](listFunct(places.toArray))

    val results = idiResults.filter(col("type") =!= "Starship").groupBy("name").agg(
      count(when(nameUDF(col("name")), true)).alias("hasName"),
      count(when(placesUDF(col("name")), true)).alias("hasPlace"),
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
      count(when(col("role") === "unknown", true)).alias("unknown"),
      dictUDF(col("name")).alias("def")
    ).filter(!(col("commonwealth") === 0 && col("hasName") > 0) || overrideUdf(col("name")))

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => (seq(3).toDouble,seq.head))
    writeDF(results, "./data/results/ships.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}

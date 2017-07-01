package com.szadowsz.cadisainmduit.places.pop

import java.io.File

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.cadisainmduit.places.OGPlacePreparer.{extractFile, writeDF}
import com.szadowsz.cadisainmduit.places.PlaceGrammar
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.StringFiller
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created on 29/06/2017.
  */
object OGPopPreparer extends LocalDataframeIO {

  val popUkSchema = Array("name", "pop")

  val brackPattern = "^(.+?)\\(.*$".r

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val pipe = new Lineage("open-gov")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> popUkSchema, "size" -> popUkSchema.length)

    //   val placesDfs = placeFiles.map(f => extractFile(sess, f, true, true)).slice(0,placeFiles.length/4)
    val placesDf = extractFile(sess, new File("./archives/data/opengov/places/popUk.csv"), false)
    val model = pipe.fit(placesDf)
    val bracUDF = udf[String,String]((s : String) => brackPattern.findFirstMatchIn(s).map(_.group(1).trim).getOrElse(s))
    val csvDF = model.transform(placesDf).select(bracUDF(col("name")).alias("name"),col("pop"))
    val result = PlaceGrammar.getRemainder(csvDF).groupBy("name").agg(max("pop"))
    writeDF(
      result,
      "./data/places/popUK.csv",
      "UTF-8",
      (row: Seq[String]) => true,
      Ordering.by((s: Seq[String]) => s.head),
      false
    )
  }

}

package com.szadowsz.cadisainmduit.people.web

import java.io.File

import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import com.szadowsz.spark.ml.feature.{ColFilterTransformer, ColRenamerTransformer, StringMapper, StringStatistics, _}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Data Preparation for name origins. Used to help ensure consistency of low count names in raw census data.
  *
  * Created on 28/01/2017.
  */
object WebNamesHandler extends LocalDataframeIO {

  /**
    * Method to provide BabyCenter specific data preparation pipeline
    *
    * @return Custom BabyCenter Pipepline
    */
  private def buildBCenterPipe(): Lineage = {
    val pipe = new Lineage("BCenter")

    // Name and separate the initial string into multiple columns
    val cols = Array("name", "gender", "bc_origin", "meaning")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)

    // Capitalise the Name to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")

    // Capitalise the Gender to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")

    // Remove uncapitalised columns
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name", "gender"), "isInclusive" -> false)

    // Rename capitalised columns
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap", "genCap"), "outputCols" -> Array("name", "gender"))

    // Remove any names that are not strictly letters
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")

    // Output debug statistics
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  /**
    * Method to provide BabyNameWizard specific data preparation pipeline
    *
    * @return Custom BabyNameWizard Pipepline
    */
  private def buildBWizardPipe(): Lineage = {
    val pipe = new Lineage("BWizard")

    // Name and separate the initial string into multiple columns
    val cols = Array("name", "gender", "pronounciation", "meaning", "variants")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)

    // Capitalise the Name to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")

    // Capitalise the Gender to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")

    // Capitalise the Variants to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "variants", "outputCol" -> "varCap", "mode" -> "all")

    // Remove uncapitalised columns
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name", "gender", "variants"), "isInclusive" -> false)

    // Rename capitalised columns
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap", "genCap", "varCap"), "outputCols" -> Array("name", "gender", "variants"))

    // Remove any names that are not strictly letters
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")

    // Output debug statistics
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  /**
    * Method to provide BehindTheName specific data preparation pipeline
    *
    * @return Custom BehindTheName Pipepline
    */
  private def buildBNamePipe(): Lineage = {
    val pipe = new Lineage("BName")

    // Name and separate the initial string into multiple columns
    val cols = Array("name", "gender", "bn_origin", "pronounciation", "variants", "meaning")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)

    // Capitalise the Name to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")

    // Capitalise the Gender to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")

    // Capitalise the Variants to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "variants", "outputCol" -> "varCap", "mode" -> "all")

    // Remove uncapitalised columns
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name", "gender", "variants"), "isInclusive" -> false)

    // Rename capitalised columns
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap", "genCap", "varCap"), "outputCols" -> Array("name", "gender", "variants"))

    // Remove any names that are not strictly letters
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")

    // Output debug statistics
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    // Get BabyCenter specific dataset
    val bcenter = extractFile(sess, new File("./data/data/web/babycenterFirstnames/babycenternames.csv"), false)
    val bcpipe = buildBCenterPipe()
    val bcmodel = bcpipe.fit(bcenter)
    val bcres = bcmodel.transform(bcenter).select("name", "gender", "bc_origin").distinct().withColumn("isBCenter", lit(1.0))
    val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(bcres, "./data/tmp/babycenter.csv", "UTF-8", (s: Seq[String]) => true, ord)

    // Get BabyNameWizard specific dataset
    val bwizard = extractFile(sess, new File("./data/data/web/babynamewizardFirstnames/babynamewizardnames.csv"), false)
    val bwpipe = buildBWizardPipe()
    val bwmodel = bwpipe.fit(bwizard)
    val bwres = bwmodel.transform(bwizard).select("name", "gender").distinct().withColumn("isBWizard", lit(1.0))
    writeDF(bwres, "./data/tmp/babynamewizard.csv", "UTF-8", (s: Seq[String]) => true, ord)

    // Get BehindTheName specific dataset
    val bname = extractFile(sess, new File("./data/data/web/behindthenameFirstnames/behindthenameFirstnames.csv"), false)
    val bnpipe = buildBNamePipe()
    val bnmodel = bnpipe.fit(bname)
    val bnres = bnmodel.transform(bname).select("name", "gender", "bn_origin").distinct().withColumn("isBNames", lit(1.0))
    writeDF(bnres, "./data/tmp/behindthename.csv", "UTF-8", (s: Seq[String]) => true, ord)

    // Read in origin mapping
    val originData = new CsvReader("./archives/dict/names/origin.csv")
    val originMap = originData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val funct = udf[String, String, String]((x: String, y: String) => Option(x).getOrElse(y))

    // Do an outer join against name and gender for all three datasets
    val all = bcres.join(bwres, Array("name", "gender"), "outer")
      .join(bnres, Array("name", "gender"), "outer")
      .na.drop(Array("name", "gender")) // drop any nulls in name or gender
      .withColumn("origin", funct(col("bc_origin"), col("bn_origin"))) // join together the two origin options
      .drop("bc_origin", "bn_origin") // drop the original origin columns
      .na.fill(0.0, Array("isBCenter", "isBWizard", "isBNames")) // fill any nulls to identify sets that do not contain that name

    // map origins
    val pipe = new Lineage("Test_WEB")
    pipe.addStage(classOf[StringMapper], Map("mapping" -> originMap, "inputCol" -> "origin", "outputCol" -> "originFinal"))
    pipe.addStage(classOf[ColFilterTransformer], Map("inputCols" -> Array("origin"), "isInclusive" -> false))
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("originFinal"), "outputCols" -> Array("origin"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    val m = pipe.fit(all)
    val res = m.transform(all)

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => (-seq(2).toDouble, -seq(4).toDouble, seq.last, -seq(3).toDouble, seq.head))
    writeDF(res, "./data/results/name_origins.csv", "UTF-8", (s: Seq[String]) => true, finalOrd)
  }
}

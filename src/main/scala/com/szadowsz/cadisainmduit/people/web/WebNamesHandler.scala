package com.szadowsz.cadisainmduit.people.web

import java.io.File

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.string.StringMapper
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColFilterTransformer, ColRenamerTransformer, CsvTransformer}
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created on 28/01/2017.
  */
object WebNamesHandler extends LocalDataframeIO {

  private def buildBCenterPipe(): Lineage = {
    val pipe = new Lineage("BCenter")
    val cols = Array("name", "gender", "bc_origin", "meaning")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name","gender"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap","genCap"), "outputCols" -> Array("name","gender"))
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  private def buildBWizardPipe(): Lineage = {
    val pipe = new Lineage("BWizard")
    val cols = Array("name", "gender", "pronounciation", "meaning", "variants")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "variants", "outputCol" -> "varCap", "mode" -> "all")
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name","gender","variants"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap","genCap","varCap"), "outputCols" -> Array("name","gender","variants"))
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  private def buildBNamePipe(): Lineage = {
    val pipe = new Lineage("BName")
    val cols = Array("name", "gender", "bn_origin", "pronounciation", "variants", "meaning")
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "gender", "outputCol" -> "genCap", "mode" -> "all")
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "variants", "outputCol" -> "varCap", "mode" -> "all")
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name","gender","variants"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap","genCap","varCap"), "outputCols" -> Array("name","gender","variants"))
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "name", "pattern" -> "[\\p{L})]+")
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    pipe
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val zips = FileFinder.search("./archives/data/web", Some(new ExtensionFilter(".zip", false))).filter(f => f.getName.endsWith("Firstnames.zip"))
    DeleteUtil.delete(new File("./data/web/tmp"))
    zips.foreach(f => ZipperUtil.unzip(f.getAbsolutePath, "./data/web/tmp/"))

    val bcenter = extractFile(sess, new File("./data/web/tmp/babycenternames.csv"), false)
    val bcpipe = buildBCenterPipe()
    val bcmodel = bcpipe.fit(bcenter)
    val bcres = bcmodel.transform(bcenter).select("name","gender","bc_origin").distinct().withColumn("isBCenter",lit(1.0))
    val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
    writeDF(bcres,"./data/web/babycenter.csv","UTF-8",(s : Seq[String]) => true,ord)

    val bwizard = extractFile(sess, new File("./data/web/tmp/babynamewizardnames.csv"), false)
    val bwpipe = buildBWizardPipe()
    val bwmodel = bwpipe.fit(bwizard)
    val bwres = bwmodel.transform(bwizard).select("name", "gender").distinct().withColumn("isBWizard",lit(1.0))
    writeDF(bwres,"./data/web/babynamewizard.csv","UTF-8",(s : Seq[String]) => true,ord)

    val bname = extractFile(sess, new File("./data/web/tmp/behindthenameFirstnames.csv"), false)
    val bnpipe = buildBNamePipe()
    val bnmodel = bnpipe.fit(bname)
    val bnres = bnmodel.transform(bname).select("name", "gender", "bn_origin").distinct().withColumn("isBNames",lit(1.0))
    writeDF(bnres,"./data/web/behindthename.csv","UTF-8",(s : Seq[String]) => true,ord)

    val originData = new CsvReader("./archives/dict/names/origin.csv")
    val originMap = originData.readAll().map(s => s.head.trim -> s.last.trim).toMap

    val funct = udf[String, String, String]((x: String, y: String) => Option(x).getOrElse(y))
    val all = bcres.join(bwres,Array("name", "gender"),"outer")
      .join(bnres,Array("name", "gender"),"outer")
      .na.drop(Array("name","gender"))
      .withColumn("origin",funct(col("bc_origin"),col("bn_origin")))
      .drop("bc_origin","bn_origin")
      .na.fill(0.0,Array("isBCenter","isBWizard","isBNames"))

    val pipe = new Lineage("Test_WEB")
    pipe.addStage(classOf[StringMapper], Map("mapping" -> originMap, "inputCol" -> "origin", "outputCol" -> "originFinal"))
    pipe.addStage(classOf[ColFilterTransformer], Map("inputCols" -> Array("origin"), "isInclusive" -> false))
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("originFinal"), "outputCols" -> Array("origin"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/web/"))
    val m = pipe.fit(all)
    val res = m.transform(all)

    val finalOrd: Ordering[Seq[String]] = Ordering.by(seq => (-seq(2).toDouble,-seq(4).toDouble,seq.last,-seq(3).toDouble,seq.head))
    writeDF(res,"./data/web/nameorigins.csv","UTF-8",(s : Seq[String]) => true, finalOrd)
    //
    //    val dfs = zips.map(f => extractFile(sess, f, true, true))
    //    val all = join(dfs)
    //
    //    val appFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("appfrac"))
    //    val avgFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("avgval"))
    //
    //    val pipe = new Lineage("XX")
    //    appFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    //    avgFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    //    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    //
    //
    //    val m = pipe.fit(all)
    //    val res = m.transform(all)
    //
    //    val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
    //    writeDF(res, s"./data/census/full_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    //
    ////    val filt = res.filter(col("UK_appFrac") >= 0.2 || col("USA_appfrac") >= 0.2)
    ////    writeDF(filt, s"./data/census/filt_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    ////
    ////    val regs = avgFields.filterNot(f => f.contains("UK"))// || f.contains("USA"))
    ////
    ////
    ////    val cond = regs.tail.foldLeft(col(regs.head) >= 20.0) { case (c, f) => c || col(f) >= 20.0 }
    ////    val filt2 = filt.drop(appFields: _*).filter(cond).select(col("name")+: col("gender") +: avgFields.map(c =>floor(col(c)).alias(c)):_*)
    ////    writeDF(filt2, "./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    ////
    //
    //    //    val pipe2 = new Lineage("XXX")
    //    //    relatives.foreach{ case(f,t) => pipe2.addStage(classOf[DivisionTransformer], "outputCol" -> s"_$f", "inputCol" -> f, "total" -> t, "decPlaces" -> 3)}
    //    //
    //    //    val m2 = pipe2.fit(filt2)
    //    //    val res2 = avgFields.foldLeft(m2.transform(filt2).drop(avgFields:_*)){case (df,f) => df.withColumnRenamed(s"_$f",f)}
    //    //    writeDF(res2, s"./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    //
  }
}

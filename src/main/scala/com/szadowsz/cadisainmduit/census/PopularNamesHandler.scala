package com.szadowsz.cadisainmduit.census

import java.io.File

import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.{CastTransformer, CsvTransformer}
import com.szadowsz.ulster.spark.transformers.math.NullTransformer
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

/**
  * Created on 28/01/2017.
  */
object PopularNamesHandler extends CensusHandler {

  val favouredOrgs = List("Ancient Greek", "Biblical", "Celtic", "Cornish", "English", "French", "German", "Latin", "Jewish", "Manx", "Roman", "Scandinavian")

  val neutralOrgs = List("American", "Ancient Egyptian", "Dutch", "Greek", "History", "Literature", "Maori", "Popular Culture", "Polynesian", "Romance", "Various")

  val unfavouredOrgs = List("African", "Armenian", "Arabic", "Chinese", "Finnish", "Georgian","Hawaiian", "Indian", "Japanese", "Native American", "Persian",
  "Turkish", "Slavic")

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val origins = extractFile(sess, new File("./data/web/nameorigins.csv"), false)
    val originsCols = Array("name", "gender", "isBCenter", "isBWizard", "isBNames", "origin")
    val originsPipe = new Lineage("origins")
    originsPipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> originsCols, "size" -> originsCols.length)

    val originsM = originsPipe.fit(origins)
    val originsRes = originsM.transform(origins)

    val files = FileFinder.search("./data/census", Some(new ExtensionFilter(".csv", false))).filter(f => f.getName.indexOf("_") == 2)
    val dfs = files.map(f => extractFile(sess, f, true, true))
    val all = join(dfs).join(originsRes, Array("name", "gender"))

    val appFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("appfrac"))
    val avgFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("avgval"))

    val pipe = new Lineage("X")
    appFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    avgFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    Array("isBCenter", "isBWizard", "isBNames").foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/census/"))


    val m = pipe.fit(all)
    val avgCols = avgFields.map(f => col(f))
    val appCols = appFields.map(f => col(f))
    val div = udf[Double, Double]((x: Double) => x / avgFields.length)
    val res = m.transform(all).withColumn("avgval_sum", avgCols.reduce(_ + _)).withColumn("avgval_avg", div(col("avgval_sum")))

    val ord: Ordering[Seq[String]] = Ordering.by(seq => (-seq(seq.length - 2).toDouble, seq.head))
    writeDF(res, s"./data/census/full_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)

    val filt = res.filter(appCols.map(c => c >= 0.2).reduce(_ || _)).drop(appFields: _*) //res.filter(col("avgval_sum") >= 10.0 || col("avgval_avg") >= 1.0)
    writeDF(filt, s"./data/census/filt_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)

    val favouredFilt = udf[Boolean, String, Double, Double]((org: String, sum: Double, avg: Double) => {
      Option(org).exists { origin =>
        val candidates = origin.split(',').map(_.trim)
        candidates.exists(s => favouredOrgs.contains(s)) && (sum >= 10.0 || avg >= 1.0)
      }
    })

    val neutralFilt = udf[Boolean, String, Double, Double]((org: String, sum: Double, avg: Double) => {
      Option(org).exists { origin =>
        val candidates = origin.split(',').map(_.trim)
        candidates.exists(s => neutralOrgs.contains(s)) && (sum >= 50.0 || avg >= 5.0)
      }
    })

    val unfavouredFilt = udf[Boolean, String, Double, Double]((org: String, sum: Double, avg: Double) => {
      Option(org).exists { origin =>
        val candidates = origin.split(',').map(_.trim)
        candidates.exists(s => unfavouredOrgs.contains(s)) && (sum >= 100.0 || avg >= 10.0)
      }
    })

    val emptyFilt = udf[Boolean, String, Double, Double]((org: String, sum: Double, avg: Double) => {
      (org == null || org.isEmpty) && (sum >= 100.0 || avg >= 10.0)
    })

    val filt2 = filt.filter(favouredFilt(col("origin"), col("avgval_sum"), col("avgval_avg")) ||
      neutralFilt(col("origin"), col("avgval_sum"), col("avgval_avg")) ||
      unfavouredFilt(col("origin"), col("avgval_sum"), col("avgval_avg")) ||
      emptyFilt(col("origin"), col("avgval_sum"), col("avgval_avg"))
    ).drop("avgval_sum","avgval_avg","isBCenter", "isBWizard", "isBNames","origin")

    writeDF(filt2, s"./data/census/filt2_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 1, Ordering.by(seq => seq.head))
    //
    //    val regs = avgFields.filterNot(f => f.contains("UK"))// || f.contains("USA"))
    //
    //
    //    val cond = regs.tail.foldLeft(col(regs.head) >= 20.0) { case (c, f) => c || col(f) >= 20.0 }
    //    val filt2 = filt.drop(appFields: _*).filter(cond).select(col("name")+: col("gender") +: avgFields.map(c =>floor(col(c)).alias(c)):_*)
    //    writeDF(filt2, "./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    //
    //    val totalCols = avgFields.map(f => sum(col(f)))
    //    val totals = filt2.agg(totalCols.head, totalCols.tail: _*).first().toSeq.map(_.asInstanceOf[Long])
    //    val writer = new CsvWriter("./data/census/final_baby_names.csv", "UTF-8", true)
    //    writer.write(Seq("total", "total") ++ totals.map(_.toString):_*)
    //    writer.close()

    //    val pipe2 = new Lineage("XXX")
    //    relatives.foreach{ case(f,t) => pipe2.addStage(classOf[DivisionTransformer], "outputCol" -> s"_$f", "inputCol" -> f, "total" -> t, "decPlaces" -> 3)}
    //
    //    val m2 = pipe2.fit(filt2)
    //    val res2 = avgFields.foldLeft(m2.transform(filt2).drop(avgFields:_*)){case (df,f) => df.withColumnRenamed(s"_$f",f)}
    //    writeDF(res2, s"./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)


  }
}

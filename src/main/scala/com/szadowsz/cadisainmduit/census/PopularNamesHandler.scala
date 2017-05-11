package com.szadowsz.cadisainmduit.census

import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CastTransformer
import com.szadowsz.ulster.spark.transformers.math.{CounterTransformer, DivisionTransformer, NullTransformer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._

/**
  * Created on 28/01/2017.
  */
object PopularNamesHandler extends CensusHandler {

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()


    val files = FileFinder.search("./data/census", Some(new ExtensionFilter(".csv", false))).filter(f => f.getName.indexOf("_") == 2)
    val dfs = files.map(f => extractFile(sess, f, true, true))
    val all = join(dfs)

    val appFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("appfrac"))
    val avgFields = all.schema.fieldNames.filter(f => f.toLowerCase.contains("avgval"))

    val pipe = new Lineage("XX")
    appFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    avgFields.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> DoubleType))
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)


    val m = pipe.fit(all)
    val res = m.transform(all)

    val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
    writeDF(res, s"./data/census/full_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)

    val filt = res.filter(col("UK_appFrac") >= 0.2 || col("USA_appfrac") >= 0.2)
    writeDF(filt, s"./data/census/filt_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)

    val regs = avgFields.filterNot(f => f.contains("UK"))// || f.contains("USA"))


    val cond = regs.tail.foldLeft(col(regs.head) >= 20.0) { case (c, f) => c || col(f) >= 20.0 }
    val filt2 = filt.drop(appFields: _*).filter(cond).select(col("name")+: col("gender") +: avgFields.map(c =>floor(col(c)).alias(c)):_*)
    writeDF(filt2, "./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)

    val totalCols = avgFields.map(f => sum(col(f)))
    val totals = filt2.agg(totalCols.head, totalCols.tail: _*).first().toSeq.map(_.asInstanceOf[Long])
    val writer = new CsvWriter("./data/census/final_baby_names.csv", "UTF-8", true)
    writer.write(Seq("total", "total") ++ totals.map(_.toString):_*)
    writer.close()

    //    val pipe2 = new Lineage("XXX")
    //    relatives.foreach{ case(f,t) => pipe2.addStage(classOf[DivisionTransformer], "outputCol" -> s"_$f", "inputCol" -> f, "total" -> t, "decPlaces" -> 3)}
    //
    //    val m2 = pipe2.fit(filt2)
    //    val res2 = avgFields.foldLeft(m2.transform(filt2).drop(avgFields:_*)){case (df,f) => df.withColumnRenamed(s"_$f",f)}
    //    writeDF(res2, s"./data/census/final_baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)


  }
}

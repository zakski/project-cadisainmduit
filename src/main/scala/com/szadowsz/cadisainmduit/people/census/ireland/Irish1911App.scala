package com.szadowsz.cadisainmduit.people.census.ireland

import java.io.File

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

object Irish1911App {

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    DeleteUtil.delete(new File("./data/census/ireland/1911/"))
    val files = FileFinder.search("./archives/data/census/ireland/1911", Some(new ExtensionFilter(".zip", false)))
    files.foreach(f => ZipperUtil.unzip(f.getAbsolutePath,"./data/census/ireland/1911"))

    val stringRdd = sess.sparkContext.textFile("./data/census/ireland/1911/census-[0-9]*.csv")
    stringRdd.cache()
    require(stringRdd.count() == 4384519)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache()

    val basePipe = RecipeUtils.buildBase(1911)
    //buildMain
    val iniPipe = RecipeUtils.buildInitials(1911) //buildMain

    var model = basePipe.fit(df)
    val base = model.transform(df)

    model = iniPipe.fit(base)
    val ini = model.transform(base)

    val extracted = base.groupBy("forename", "sex").agg(
      count(when(col("age").equalTo("0.0"), true)).alias("appr_1"),
      count(when(col("age").equalTo("1.0"), true)).alias("appr_2"),
      count(when(col("age").equalTo("2.0"), true)).alias("appr_3"),
      count(when(col("age").equalTo("3.0"), true)).alias("appr_4"),
      count(when(col("age").equalTo("4.0"), true)).alias("appr_5"),
      count(when(col("age").equalTo("5.0"), true)).alias("appr_6"),
      count(when(col("age").equalTo("6.0"), true)).alias("appr_7")
    ).filter(col("forename").rlike("^([A-Z])+$")).filter(col("sex").rlike("^[MF]$"))

    val resPipe = RecipeUtils.buildFractionPipeline(1911, extracted.columns.filter(f => f.startsWith("appr")))
    val resModel = resPipe.fit(extracted)
    val result = resModel.transform(extracted)

    val writer = new CsvWriter("./data/census/ire_1911_baby_names.csv", "UTF-8", false)
    writer.write(result.schema.fieldNames: _*)
    val res = result.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
    writer.writeAll(res.sortBy(seq => (seq.head)))
    writer.close()
  }
}

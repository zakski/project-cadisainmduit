package com.szadowsz.cadisainmduit.people.census.canada

import java.io.File

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature._
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.sql.types.{IntegerType, NumericType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created on 16/01/2017.
  */
object CanadaNamesFreqCalculator extends CensusHandler {

  def agg (df : DataFrame, reg : String) : DataFrame = {
    val appFields = df.schema.fieldNames.filterNot(f => f == "name" || f == "gender")
    val pipe: Lineage = buildFractionPipeline(s"$reg-frac",reg, appFields,appFields)
    val m = pipe.fit(df)
    m.transform(df)
  }

  def loadData(sess: SparkSession, path: String) = {
    val f = new File(path)
    val cols = extractSchema(f)
    val stringDF = extractFile(sess, f, true, false)

    val pipe = new Lineage("load")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    cols.filter(c => c != "name" && c != "gender").foreach(c => pipe.addStage(classOf[CastTransformer], "inputCol" -> c, "outputDataType" -> IntegerType))

    val mod = pipe.fit(stringDF)
    mod.transform(stringDF)
  }

  def loadData(save: Boolean): DataFrame = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val alb = agg(loadData(sess, "./data/tmp/AB/baby_names.csv"),"AB")
    val bc = agg(loadData(sess, "./data/tmp/BC/baby_names.csv"),"BC")
    val ot = agg(loadData(sess, "./data/tmp/OT/baby_names.csv"),"OT")

    val cn = alb.join(bc, Seq("name", "gender"), "outer").join(ot, Seq("name", "gender"), "outer").na.fill("unused")

    val numerics = cn.schema.filter(f => f.dataType.isInstanceOf[NumericType]).map(_.name)
    val fracs = cn.schema.map(_.name).filter(_.contains("AppRank"))
    val avg = numerics.filter(_.contains("avgVal"))

    val pipe = new Lineage("cn")
    fracs.foreach(r => pipe.addStage(new StringIndexerModel(s"$r-Indexer",Array("unused","rare","uncommon","common","basic")).setInputCol(r).setOutputCol(r + "Ind")))
    pipe.addStage(classOf[AverageTransformer],"inputCols" -> fracs.map(_ + "Ind").toArray, "outputCol" -> "CN_RawAppRank")
    pipe.addStage(classOf[RoundingTransformer],"inputCol" -> "CN_RawAppRank", "outputCol" -> "CN_RoundedAppRank")
    pipe.addStage(classOf[IndexToString],"inputCol" -> "CN_RoundedAppRank", "outputCol" -> "CN_AppRank", "labels" -> Array("unused","rare","uncommon","common","basic"))
    val (model,cnDf) = pipe.fitAndTransform(cn)
    var df = cnDf.select("name","gender", "CN_AppRank")

    if (save) {
      val writer = new CsvWriter("./data/results/cn_baby_names.csv", "UTF-8", false)
      writer.write(df.schema.fieldNames: _*)
      val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      writer.writeAll(res.sortBy(seq => (seq.head)))
      //writer.write(tots: _*)
      writer.close()
    }

    df
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

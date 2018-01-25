package com.szadowsz.cadisainmduit.people.census.usa

import java.io.File

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.cadisainmduit.people.census.uk.engwales.EngWalNamesFreqCalculator
import com.szadowsz.cadisainmduit.people.census.uk.norire.NorireNamesFreqCalculator
import com.szadowsz.cadisainmduit.people.census.uk.scotland.ScotNamesFreqCalculator
import com.szadowsz.cadisainmduit.people.census.uk.scotland.ScotNamesFreqCalculator.{extractFile, extractSchema}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature._
import org.apache.spark.ml.feature.{Bucketizer, IndexToString, StringIndexerModel}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created on 16/01/2017.
  */
object UsaCensusNamesFreqCalculator extends CensusHandler{

  override protected def buildFractionPipeline(name: String, country: String, appCols: Array[String], popCols: Array[String]): Lineage = {
    val pipe = new Lineage(name)
    pipe.addStage(classOf[ValueCounter], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> s"${country}_appearCount")

    val div = Map("outputCol" -> s"${country}_appFrac", "inputCol" -> s"${country}_appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)
    pipe.addStage(classOf[Bucketizer], "inputCol" ->  s"${country}_appFrac", "outputCol" -> s"${country}_App",
      "splits" -> Array(Double.NegativeInfinity, 0.25, 0.5, 0.85, Double.PositiveInfinity))
    pipe.addStage(classOf[IndexToString], "inputCol" -> s"${country}_App", "outputCol" -> s"${country}_AppRank", "labels" -> Array("rare","uncommon","common","basic"))
    val excluded = Array(s"${country}_appFrac",s"${country}_App", s"${country}_appearCount" /*, "counts"*/) ++ appCols
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> excluded, "isInclusive" -> false)
    pipe
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

    val df1 = loadData(sess, "./data/tmp/USA/baby_names_1800_until_1918.csv")
    val df2 = loadData(sess, "./data/tmp/USA/baby_names_1918_until_1945.csv")
    val df3 = loadData(sess, "./data/tmp/USA/baby_names_1945_until_1990.csv")
    val df4 = loadData(sess, "./data/tmp/USA/baby_names_1990_until_2050.csv")

    val rawUS = join(List(df1,df2,df3,df4))

    val appFields = rawUS.schema.fieldNames.filterNot(f => f == "name" || f == "gender")
    val pipe: Lineage = buildFractionPipeline(s"usa-frac","USA", appFields,appFields)
    val (_,children) = pipe.fitAndTransform(rawUS)

    if (save) {
      val writer = new CsvWriter("./data/results/usa_baby_names.csv", "UTF-8", false)
      writer.write(children.schema.fieldNames: _*)
      val res = children.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      writer.writeAll(res.sortBy(seq => (seq.head)))
      //writer.write(tots: _*)
      writer.close()
    }
    children
  }
  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

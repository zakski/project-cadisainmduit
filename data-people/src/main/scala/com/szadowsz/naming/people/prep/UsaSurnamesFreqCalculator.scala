package com.szadowsz.naming.people.prep

import java.io.File

import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature._
import org.apache.spark.ml.feature.{Bucketizer, IndexToString}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Built to reduce us surname stats to something processable
  */
object UsaSurnamesFreqCalculator extends CensusHandler {

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

    val children = loadData(sess, "./data/tmp/NI/baby_names.csv")

    val appFields = children.schema.fieldNames.filterNot(f => f == "name" || f == "gender")
    val pipe: Lineage = buildFractionPipeline(s"ni-frac","NI", appFields,appFields)
    val (_,result) = pipe.fitAndTransform(children)

   if (save) {
      val writer = new CsvWriter("./data/results/ni_baby_names.csv", "UTF-8", false)
      writer.write(result.schema.fieldNames: _*)
      val res = result.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      writer.writeAll(res.sortBy(seq => (seq.head)))
      //writer.write(tots: _*)
      writer.close()
    }
    result
  }
  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


package com.szadowsz.naming.people.prep.us

import com.szadowsz.naming.people.prep.CensusFileCombiner
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature.ColFilterTransformer
import org.apache.spark.ml.feature.{Bucketizer, IndexToString}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object UsaSurnamesCombiner {
  def main(args: Array[String]): Unit = {
    new UsaSurnamesCombiner().loadData(true)
  }
}

class UsaSurnamesCombiner extends CensusFileCombiner {
 
  /**
    * Public method to load in the segregated dataset
    *
    * @param save whether or not to dump the prepared data into a temp file.
    * @return the prepared dataset.
    */
  override def loadData(save: Boolean): DataFrame = {
    loadAndCombineData(save,"surnames","USA","./data/data/census/us/surnames-2000-2010")

  }

  override def loadFilesData(region: String, root: String, sess: SparkSession): DataFrame = {
    val data = super.loadFilesData(region, root, sess)
      .na.fill("0")

    val headers = data.columns.tail.groupBy(s => s.substring(0, s.lastIndexOf("_")))
    val sortedHeaders = headers.keys.toList.sorted
    val combined = headers.foldLeft(data) { case (d, (k, v)) => d.withColumn(k, round(v.map(col).reduce(_ + _) / 2,2)) }
    val merged = combined.select("name", sortedHeaders: _*)

    val pipeline = buildCountPipeline("pctUSA", sortedHeaders.filter(_.contains("count")).head)
    pipeline.fitAndTransform(merged)._2.filter(col("USA_Rank") =!= "unused").select("name", "USA_Rank")
  }

    override protected def selectCols(region: String, year: String, schema: Array[String], tmp: DataFrame): Dataset[Row] = {
    tmp.select("name",schema.filter(h => h.contains("pct") || h.contains("count")):_*)
      .na.replace("*",Map("(S)" -> "0"))
  }

  protected def buildCountPipeline(name: String, countCol : String): Lineage = {
    val pipe = new Lineage(name)
    pipe.addStage(classOf[Bucketizer], "inputCol" -> countCol, "outputCol" -> s"${countCol}_buckets",
              "splits" -> Array(Double.NegativeInfinity, 100,1000, 10000, 100000, 500000, Double.PositiveInfinity))

    pipe.addStage(classOf[IndexToString], "inputCol" -> s"${countCol}_buckets", "outputCol" -> "USA_Rank",
      "labels" -> Array("unused","ultra-rare","rare","uncommon","common","basic"))

    val excluded = Array(s"${countCol}_buckets")
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> excluded, "isInclusive" -> false)
    pipe
  }


  //  protected def buildPctPipeline(name: String, input: Seq[String]): Lineage = {
//    val pipe = new Lineage(name)
//
//    input.foreach{ i =>
//      pipe.addStage(classOf[Bucketizer], "inputCol" -> i, "outputCol" -> s"${i}_bucket",
//        "splits" -> Array(Double.NegativeInfinity, 1, 10, 25, 50, Double.PositiveInfinity))
//    }
//
//    input.foreach { i =>
//      pipe.addStage(classOf[IndexToString], "inputCol" -> s"${i}_bucket", "outputCol" -> s"${i}_cats",
//        "labels" -> Array("unused","rare", "uncommon", "common", "basic"))
//    }
//
//    val excluded = input ++ input.map(i => s"${i}_bucket")
//    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> excluded.toArray, "isInclusive" -> false)
//    pipe
//  }
}

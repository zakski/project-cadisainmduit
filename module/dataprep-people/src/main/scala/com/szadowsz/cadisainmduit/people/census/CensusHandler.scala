package com.szadowsz.cadisainmduit.people.census

import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import com.szadowsz.spark.ml.feature._
import org.apache.spark.ml.feature.{IndexToString, QuantileDiscretizer}
import org.apache.spark.sql.DataFrame

/**
  * Created on 25/01/2017.
  */
trait CensusHandler extends LocalDataframeIO {

  /**
    * Method to build standard mono-gender dataset
    *
    * @param name   the name of the pipeline
    * @param cols   the expected initial columns
    * @return
    */
  protected def buildStdPipeline(name: String, cols: Array[String]): Lineage = {
    val pipe = new Lineage(name)

    // Name and separate the initial string into multiple columns
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)

    // Capitalise the Name to ensure consistency
    pipe.addStage(classOf[StringCapitaliser], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")

    // Remove any names that are not strictly letters
    pipe.addStage(classOf[RegexValidator], "inputCol" -> "nameCap", "pattern" -> "^\\p{L}+$")

    // Remove uncapitalised name column
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name"), "isInclusive" -> false)

    // Rename capitalised column
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap"), "outputCols" -> Array("name"))
    pipe
  }

  protected def buildFractionPipeline(name: String, country: String, appCols: Array[String], popCols: Array[String]): Lineage = {
    val pipe = new Lineage(name)
    pipe.addStage(classOf[ValueCounter], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> s"${country}_appearCount")

    val div = Map("outputCol" -> s"${country}_appFrac", "inputCol" -> s"${country}_appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)
    pipe.addStage(classOf[QuantileDiscretizer], "inputCol" -> s"${country}_appFrac", "outputCol" -> s"${country}_App", "numBuckets" -> 4)
    pipe.addStage(classOf[IndexToString], "inputCol" -> s"${country}_App", "outputCol" -> s"${country}_AppRank", "labels" -> Array("rare","uncommon","common","basic"))
    val excluded = Array(s"${country}_appFrac",s"${country}_App", s"${country}_appearCount" /*, "counts"*/) ++ appCols
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> excluded, "isInclusive" -> false)
    pipe
  }


  /**
    * Method to join a sequence of personal datasets together, using name and gender as keys
    *
    * @param dfs sequence of datasets
    * @return combined dataset
    */
  protected def join(dfs: Seq[DataFrame]): DataFrame = {
    require(dfs.forall(df => df.schema.fieldNames.contains("name") && df.schema.fieldNames.contains("gender")), "Missing Default Column")
    dfs.tail.foldLeft(dfs.head) { case (comp, curr) => comp.join(curr, Seq("name", "gender"), "outer") }
  }
}
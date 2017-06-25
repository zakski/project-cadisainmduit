package com.szadowsz.cadisainmduit.people.census

import java.io.{File, StringReader}

import com.szadowsz.cadisainmduit.LocalDataframeIO
import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import com.szadowsz.ulster.spark.transformers.math.{CounterTransformer, DivisionTransformer, NullTransformer}
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColFilterTransformer, ColRenamerTransformer, CsvTransformer}
import com.szadowsz.ulster.spark.transformers.string.StringFiller
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 25/01/2017.
  */
trait CensusHandler extends LocalDataframeIO {

  protected def buildStdPipeline(name: String, cols: Array[String], gender: Option[Char]): Lineage = {
    require(cols.contains("name") && (cols.contains("gender") || (gender.contains('M') || gender.contains('F'))), "Missing Default Column")
    val pipe = new Lineage(name)
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "nameCap", "pattern" -> "^\\p{L}+$")
    gender.foreach(g => pipe.addStage(classOf[StringFiller], "outputCol" -> "gender", "value" -> g.toString))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap"), "outputCols" -> Array("name"))
    pipe
  }

  protected def buildFractionPipeline(name: String, country: String, appCols: Array[String], popCols: Array[String]): Lineage = {
    val pipe = new Lineage(name)
    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[CounterTransformer], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> "appearCount")

    val div = Map("outputCol" -> s"${country}_appFrac", "inputCol" -> "appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)

    popCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], "inputCols" -> popCols, "outputCol" -> "counts")
    pipe.addStage(classOf[AverageTransformer], "inputCol" -> "counts", "excludeZeros" -> true, "outputCol" -> s"${country}_avgVal", "decPlaces" -> 2)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> (Array("appearCount", "counts") ++ appCols), "isInclusive" -> false)
    pipe
  }


  protected def join(dfs: Seq[DataFrame]): DataFrame = {
    require(dfs.forall(df => df.schema.fieldNames.contains("name") && df.schema.fieldNames.contains("gender")), "Missing Default Column")
    dfs.tail.foldLeft(dfs.head) { case (comp, curr) => comp.join(curr, Seq("name", "gender"), "outer") }
  }
}
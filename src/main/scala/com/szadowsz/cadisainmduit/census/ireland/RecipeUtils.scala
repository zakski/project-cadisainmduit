package com.szadowsz.cadisainmduit.census.ireland

import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers._
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import com.szadowsz.ulster.spark.transformers.math.{CounterTransformer, DivisionTransformer, NullTransformer}
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, CatSpellingTransformer, RegexValidationTransformer, WordSpellingTransformer}
import com.szadowsz.ulster.spark.transformers.string._
import com.szadowsz.ulster.spark.transformers.util.stats.{CrossTabTransformer, StringStatistics}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Bucketizer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

/**
  * Created on 29/11/2016.
  */
object RecipeUtils {

  val fieldsListMap = Map(1901 -> Array(
    "surname",
    "forename",
    "townlandOrStreet",
    "DED",
    "county",
    "age",
    "sex",
    "birthplace",
    "occupation",
    "religion",
    "literacy",
    "languages",
    "relationToHead",
    "married",
    "illnesses",
    "house"
  ),
    1911 -> Array(
      "surname",
      "forename",
      "townlandOrStreet",
      "DED",
      "county",
      "age",
      "sex",
      "birthplace",
      "occupation",
      "religion",
      "literacy",
      "languages",
      "relationToHead",
      "married",
      "illnesses",
      "yearsMarried",
      "childrenBorn",
      "childrenLiving",
      "house"
    ))

  def buildBase(year : Int): Pipeline = {
    val pipe = new Lineage(s"$year-Base")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> fieldsListMap(year), "size" -> fieldsListMap(year).length))
    pipe.addStage(classOf[ColFilterTransformer], Map("isInclusive" -> true, "inputCols" -> Array("surname","forename","age","sex")))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "surname", "outputCol" -> "surCap", "mode" -> "all"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "forename", "outputCol" -> "forCap", "mode" -> "all"))
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "age", "outputDataType" -> DoubleType)
    pipe.addStage(classOf[Bucketizer], "inputCol" -> "age",
      "outputCol" -> "ageBucks",
      "splits" -> Array(Double.NegativeInfinity, 16.0, 21.0, 30.0, 40.0, 50.0, 60.0, Double.PositiveInfinity))
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "ageBucks", "outputDataType" -> StringType)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("age", "surname","forename"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("surCap","forCap","ageBucks"), "outputCols" -> Array("surname","forename","age"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/side/"))
  }

  def buildInitials(year : Int): Pipeline = {
    val pipe = new Lineage(s"$year-Initials")
    pipe.addStage(classOf[InitialisationTransformer], "inputCol" -> "surname", "outputCol" -> "surI")
    pipe.addStage(classOf[InitialisationTransformer], "inputCol" -> "forename", "outputCol" -> "forI")
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surI", "pattern" -> "[A-Z]"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forI", "pattern" -> "[A-Z]"))
    pipe.addPassThroughTransformer(classOf[CrossTabTransformer], Map("inputCols" -> Array("surI","forI"), "isDebug" -> true, "debugPath" -> "./data/debug/side/"))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("surI","forI"), "isInclusive" -> false)
  }

  def buildFractionPipeline(year : Int, appCols: Array[String]): Lineage = {
    val pipe = new Lineage(s"$year-Frac")
    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[CounterTransformer], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> "appearCount")

    val div = Map("outputCol" -> s"IRE${year}_appFrac", "inputCol" -> "appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)

    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], "inputCols" -> appCols, "outputCol" -> "counts")
    pipe.addStage(classOf[AverageTransformer], "inputCol" -> "counts", "excludeZeros" -> true, "outputCol" -> s"IRE${year}_avgVal", "decPlaces" -> 2)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> (Array("appearCount", "counts") ++ appCols), "isInclusive" -> false)
    pipe
  }
}

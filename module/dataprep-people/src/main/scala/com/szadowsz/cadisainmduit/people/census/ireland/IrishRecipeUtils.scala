package com.szadowsz.cadisainmduit.people.census.ireland

import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Bucketizer, IndexToString, QuantileDiscretizer, VectorAssembler}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

/**
  * Created on 29/11/2016.
  */
object IrishRecipeUtils {

  val fieldsListMap = Map(1901 -> Array(
    "surname",
    "name",
    "townlandOrStreet",
    "DED",
    "county",
    "age",
    "gender",
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
      "name",
      "townlandOrStreet",
      "DED",
      "county",
      "age",
      "gender",
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
    pipe.addStage(classOf[CsvColumnExtractor], Map("inputCol" -> "fields", "outputCols" -> fieldsListMap(year), "size" -> fieldsListMap(year).length))
    pipe.addStage(classOf[ColFilterTransformer], Map("isInclusive" -> true, "inputCols" -> Array("surname","name","age","gender")))
    pipe.addStage(classOf[RegexValidator], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidator], Map("inputCol" -> "name", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[StringCapitaliser], Map("inputCol" -> "surname", "outputCol" -> "surCap", "mode" -> "all"))
    pipe.addStage(classOf[StringCapitaliser], Map("inputCol" -> "name", "outputCol" -> "forCap", "mode" -> "all"))
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "age", "outputDataType" -> DoubleType)
    pipe.addStage(classOf[Bucketizer], "inputCol" -> "age",
      "outputCol" -> "ageBucks",
      "splits" -> Array(Double.NegativeInfinity, 16.0, 21.0, 30.0, 40.0, 50.0, 60.0, Double.PositiveInfinity))
    pipe.addStage(classOf[CastTransformer], "inputCol" -> "ageBucks", "outputDataType" -> StringType)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("age", "surname","name"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("surCap","forCap","ageBucks"), "outputCols" -> Array("surname","name","age"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/side/"))
  }

  def buildInitials(year : Int): Pipeline = {
    val pipe = new Lineage(s"$year-Initials")
    pipe.addStage(classOf[StringInitialiser], "inputCol" -> "surname", "outputCol" -> "surI")
    pipe.addStage(classOf[StringInitialiser], "inputCol" -> "name", "outputCol" -> "forI")
    pipe.addStage(classOf[RegexValidator], Map("inputCol" -> "surI", "pattern" -> "[A-Z]"))
    pipe.addStage(classOf[RegexValidator], Map("inputCol" -> "forI", "pattern" -> "[A-Z]"))
    pipe.addPassThroughTransformer(classOf[CrossTabTransformer], Map("inputCols" -> Array("surI","forI"), "isDebug" -> true, "debugPath" -> "./data/debug/side/"))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("surI","forI"), "isInclusive" -> false)
  }

  def buildFractionPipeline(year : Int, appCols: Array[String]): Lineage = {
    val pipe = new Lineage(s"$year-Frac")
    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[ValueCounter], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> "appearCount")

    val div = Map("outputCol" -> s"IRE${year}_appFrac", "inputCol" -> "appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)
    pipe.addStage(classOf[Bucketizer], "inputCol" -> s"IRE${year}_appFrac", "outputCol" -> s"IRE${year}_App",
      "splits" -> Array(Double.NegativeInfinity, 0.25, 0.5, 0.85, Double.PositiveInfinity))
 //   pipe.addStage(classOf[QuantileDiscretizer], "inputCol" -> s"IRE${year}_appFrac", "outputCol" -> s"IRE${year}_App", "numBuckets" -> 4)
    pipe.addStage(classOf[IndexToString], "inputCol" -> s"IRE${year}_App", "outputCol" -> s"IRE${year}_AppRank", "labels" -> Array("rare","uncommon","common","basic"))
//
//    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
//    pipe.addStage(classOf[NullReplacer], "replacement" -> 0.0)
//    pipe.addStage(classOf[AverageTransformer], "inputCols" -> appCols, "outputCol" -> s"IRE${year}_avgVal")
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> (Array("appearCount", s"IRE${year}_appFrac", s"IRE${year}_App") ++ appCols), "isInclusive" -> false)
    pipe
  }
}

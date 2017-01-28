package com.szadowsz.cadisainmduit.census.ireland

import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.util.stats.StringStatistics
import com.szadowsz.ulster.spark.transformers._
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, CatSpellingTransformer, RegexValidationTransformer, WordSpellingTransformer}
import com.szadowsz.ulster.spark.transformers.string.{StringMapper, StringMappingGenerator, StructToString, TokeniserTransformer}
import com.szadowsz.common.io.read.CsvReader
import org.apache.spark.ml.Pipeline

/**
  * Created on 29/11/2016.
  */
object RecipeUtils {

  val fieldsList = Array(
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
  )

  def buildLitMappingOptimisation: Pipeline = {

    val lit = new CsvReader("./data/dict/lit.csv")
    val litStr = lit.readAll().map(_.head)

    val litterms = new CsvReader("./data/dict/litterms.csv")
    val litMp = litterms.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "literacy", "outputCol" -> "litCap"))
    pipe.addStage(classOf[TokeniserTransformer], Map("inputCol" -> "litCap", "outputCol" -> "litTokens"))
    pipe.addStage(classOf[WordSpellingTransformer], Map("inputCol" -> "litTokens", "outputCol" -> "litSpelt", "dictionary" -> litStr.toArray))
    pipe.addStage(classOf[StructToString], Map("inputCol" -> "litSpelt", "outputCol" -> "litFilt"))
    pipe.addStage(classOf[CatSpellingTransformer], Map("inputCol" -> "litFilt", "outputCol" -> "litFinal", "dictionary" -> litMp, "limit" -> 5))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/"))
    pipe.addPassThroughTransformer(classOf[StringMappingGenerator], Map("isDebug" -> true, "debugPath" -> "./data/dict/lit", "inputCols" -> Array("literacy",
      "litFinal")))
  }

  def buildMarMappingOptimisation: Pipeline = {

    val ma = new CsvReader("./data/dict/ma.csv")
    val maStr = ma.readAll().map(_.head)

    val marrterms = new CsvReader("./data/dict/marrterms.csv")
    val maMp = marrterms.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "married", "outputCol" -> "marrCap"))
    pipe.addStage(classOf[TokeniserTransformer], Map("inputCol" -> "marrCap", "outputCol" -> "marrTokens"))
    pipe.addStage(classOf[WordSpellingTransformer], Map("inputCol" -> "marrTokens", "outputCol" -> "marSpelt", "dictionary" -> maStr.toArray))
    pipe.addStage(classOf[StructToString], Map("inputCol" -> "marSpelt", "outputCol" -> "marFilt"))
    pipe.addStage(classOf[CatSpellingTransformer], Map("inputCol" -> "marFilt", "outputCol" -> "maFinal", "dictionary" -> maMp, "limit" -> 5))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/"))
    pipe.addPassThroughTransformer(classOf[StringMappingGenerator], Map("isDebug" -> true, "debugPath" -> "./data/dict/marr/", "inputCols" -> Array("married",
      "maFinal")))
  }

  def buildIllMappingOptimisation: Pipeline = {

    val ma = new CsvReader("./data/dict/ill.csv")
    val maStr = ma.readAll().map(_.head)

    val marrterms = new CsvReader("./data/dict/illterms.csv")
    val maMp = marrterms.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "illnesses", "outputCol" -> "illCap"))
    pipe.addStage(classOf[TokeniserTransformer], Map("inputCol" -> "illCap", "outputCol" -> "illTokens"))
    pipe.addStage(classOf[WordSpellingTransformer], Map("inputCol" -> "illTokens", "outputCol" -> "illSpelt", "dictionary" -> maStr.toArray))
    pipe.addStage(classOf[StructToString], Map("inputCol" -> "illSpelt", "outputCol" -> "illFilt"))
    pipe.addStage(classOf[CatSpellingTransformer], Map("inputCol" -> "illFilt", "outputCol" -> "illFinal", "dictionary" -> maMp, "limit" -> 5))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/"))
    pipe.addPassThroughTransformer(classOf[StringMappingGenerator], Map("isDebug" -> true, "debugPath" -> "./data/dict/ill/", "inputCols" -> Array("illnesses",
      "illFinal")))
  }

  def buildLangMappingOptimisation: Pipeline = {

    val ma = new CsvReader("./data/dict/lang.csv")
    val maStr = ma.readAll().map(_.head)

    val marrterms = new CsvReader("./data/dict/langTerms.csv")
    val maMp = marrterms.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "languages", "outputCol" -> "langCap"))
    pipe.addStage(classOf[TokeniserTransformer], Map("inputCol" -> "langCap", "outputCol" -> "langTokens"))
    pipe.addStage(classOf[WordSpellingTransformer], Map("inputCol" -> "langTokens", "outputCol" -> "langSpelt", "dictionary" -> maStr.toArray))
    pipe.addStage(classOf[StructToString], Map("inputCol" -> "langSpelt", "outputCol" -> "langFilt"))
    pipe.addStage(classOf[CatSpellingTransformer], Map("inputCol" -> "langFilt", "outputCol" -> "langFinal", "dictionary" -> maMp, "limit" -> 5))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/"))
    pipe.addPassThroughTransformer(classOf[StringMappingGenerator], Map("isDebug" -> true, "debugPath" -> "./data/dict/lang/", "inputCols" -> Array("languages",
      "langFinal")))
  }

  def buildRelMappingOptimisation: Pipeline = {

    val ma = new CsvReader("./data/dict/rel.csv")
    val maStr = ma.readAll().map(_.head)

        val marrterms = new CsvReader("./data/dict/relTerms.csv")
        val maMp = marrterms.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("Side")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> Array("religion","count"), "size" -> 2))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "religion", "outputCol" -> "relCap"))
    pipe.addStage(classOf[TokeniserTransformer], Map("inputCol" -> "relCap", "outputCol" -> "relTokens"))
    pipe.addStage(classOf[WordSpellingTransformer], Map("inputCol" -> "relTokens", "outputCol" -> "relSpelt", "dictionary" -> maStr.toArray))
    pipe.addStage(classOf[StructToString], Map("inputCol" -> "relSpelt", "outputCol" -> "relFilt"))
      pipe.addStage(classOf[CatSpellingTransformer], Map("inputCol" -> "relFilt", "outputCol" -> "relFinal", "dictionary" -> maMp, "limit" -> 5))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/"))
        pipe.addPassThroughTransformer(classOf[StringMappingGenerator], Map("isDebug" -> true, "debugPath" -> "./data/dict/rel/", "inputCols" -> Array
        ("religion",
          "relFinal")))
  }

  def buildWords: Pipeline = {
    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "literacy", "outputCol" -> "litCap"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "occupation", "outputCol" -> "occuCap"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "religion", "outputCol" -> "relCap"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "married", "outputCol" -> "maCap"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "languages", "outputCol" -> "langCap"))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "illnesses", "outputCol" -> "illCap"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/side/"))
  }

  def buildMain: Pipeline = {

    val lit = new CsvReader("./data/dict/lit/mapping.csv")
    val litMp = lit.readAll().map(s => s.head -> s.last).toMap

    val mar = new CsvReader("./data/dict/marr/mapping.csv")
    val marMp = mar.readAll().map(s => s.head -> s.last).toMap

    val ill = new CsvReader("./data/dict/ill/mapping.csv")
    val illMp = ill.readAll().map(s => s.head -> s.last).toMap

    val lang = new CsvReader("./data/dict/lang/mapping.csv")
    val langMp = lang.readAll().map(s => s.head -> s.last).toMap

    val rel = new CsvReader("./data/dict/rel/mapping.csv")
    val relMp = rel.readAll().map(s => s.head -> s.last).toMap

    val pipe = new Lineage("1901")
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> RecipeUtils.fieldsList, "size" -> RecipeUtils.fieldsList.length))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "surname", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "forename", "pattern" -> "\\p{L}[\\p{L} '-]*"))
    pipe.addStage(classOf[StringMapper], Map("mapping" -> litMp, "inputCol" -> "literacy", "outputCol" -> "litFinal"))
    pipe.addStage(classOf[StringMapper], Map("mapping" -> marMp, "inputCol" -> "married", "outputCol" -> "marFinal"))
    pipe.addStage(classOf[StringMapper], Map("mapping" -> illMp, "inputCol" -> "illnesses", "outputCol" -> "illFinal"))
    pipe.addStage(classOf[StringMapper], Map("mapping" -> langMp, "inputCol" -> "languages", "outputCol" -> "langFinal"))
    pipe.addStage(classOf[StringMapper], Map("mapping" -> relMp, "inputCol" -> "religion", "outputCol" -> "relFinal"))
    pipe.addPassThroughTransformer(classOf[StringStatistics], Map("isDebug" -> true, "debugPath" -> "./data/debug/main/"))
  }
}

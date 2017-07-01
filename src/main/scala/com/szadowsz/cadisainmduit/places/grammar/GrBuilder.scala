package com.szadowsz.cadisainmduit.places.grammar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created on 29/06/2017.
  */
object GrBuilder {

  val gramList: List[GrTerm] = List(
    PrefixSufffixGrTerm("Age", List("New", "Old"), true, 1),
    PrefixSufffixGrTerm("Cardinal", List("North", "South", "East", "West", "Northeast", "Northwest", "Southeast", "Southwest", "Northern", "Southern", "Eastern",
      "Western"), true, 1),
    PrefixSufffixGrTerm("Cardinal", List("North", "South", "East", "West", "Northeast", "Northwest", "Southeast", "Southwest", "Northern", "Southern", "Eastern",
      "Western"), false, 1),
    PrefixSufffixGrTerm("Great", List("Great", "Greater"), true, 1),
    PrefixSufffixGrTerm("Low", List("Low", "Lower"), true, 1),
    PrefixSufffixGrTerm("Size", List("Big", "Little"), true, 1),
    PrefixSufffixGrTerm("High", List("High"), true, 1),
    PrefixSufffixGrTerm("Regal", List("Royal"), true, 1),
    PrefixSufffixGrTerm("Regal", List("Regis"), false, 1),
    PrefixSufffixGrTerm("Pre", List("Mount","Camp","Fort","Port","Point"), true, 2)
  )

  val lvlNames = List("<<PLACENAME>>", "<<BODY>>", "<<CORE>>")

  protected def extractDF(df: DataFrame, colName: String, func: UserDefinedFunction): DataFrame = {
    df.select(func(col(colName)).alias(colName) +: df.columns.filterNot(_ == colName).map(s => col(s)): _*)
  }

  protected def getGrammarAtLvl(pres: Int): List[GrTerm] = gramList.filter(g => g.precedence == pres)

  protected def groupUp(df: DataFrame, colName: String): DataFrame = {
    val sums = df.columns.filterNot(_ == colName).map(c => sum(col(c)).alias(c))
    df.groupBy(colName).agg(sums.head, sums.tail: _*)
  }

  protected def conformUdfAtLevel(pres: Int, child: String): UserDefinedFunction = {
    val lvlGr = getGrammarAtLvl(pres)
    udf[String, String]((s: String) => {
      val mapped = lvlGr.map(g => g.conform(child, s)).find(_.isDefined).flatten
      mapped.getOrElse(child)
    })
  }

  protected def remainderUdfAtLevel(pres: Int): UserDefinedFunction = {
    val lvlGr = getGrammarAtLvl(pres)
    udf[String, String]((s: String) => {
      val mapped = lvlGr.map(g => g.remainder(s)).find(_.isDefined).flatten
      mapped.getOrElse(s)
    })
  }

  protected def extractLvl(df: DataFrame, placeholder: String, lvl: Int): (DataFrame, DataFrame) = {
    val parent = lvlNames(lvl-1)
    val parentUDF = udf(() => parent)
    val head = groupUp(extractDF(df, "name", conformUdfAtLevel(lvl, placeholder)), "name").withColumn("parent", parentUDF())
    val body = extractDF(df, "name", remainderUdfAtLevel(lvl))
    (head, body)
  }

  protected def extract(df: DataFrame, lvl: Int, max: Int): (DataFrame, DataFrame) = {
    val (h1, b1) = extractLvl(df, lvlNames(lvl), lvl)
    if (lvl < max) {
      val (h2, b2) = extract(df, lvl + 1, max)
      (h1.union(h2), b1.union(b2))
    } else {
      (h1, b1)
    }
  }

  def extract(df: DataFrame): (DataFrame, DataFrame) = {
    val max = gramList.map(_.precedence).max
    extract(df, 1, max)
  }
}

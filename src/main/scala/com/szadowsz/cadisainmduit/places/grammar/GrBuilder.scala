package com.szadowsz.cadisainmduit.places.grammar

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}

/**
  * Created on 29/06/2017.
  */
case class GrBuilder(gramList : List[GrTerm], lvlNames : List[String], filterUdf : Option[UserDefinedFunction], defaultTerminus : String, terminus : Map[String,String]) {


  protected val terminusMappingUDF: UserDefinedFunction = udf[String, String]((name: String) => terminus.getOrElse(name,s"<<${defaultTerminus.toUpperCase}>>"))
  protected val remainderUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => !terminus.contains(name))

  protected def getGrammarAtLvl(pres: Int): List[GrTerm] = gramList.filter(g => g.precedence == pres)

  protected def extractDF(df: DataFrame, colName: String, func: UserDefinedFunction): DataFrame = {
    df.select(func(col(colName)).alias(colName) +: df.columns.filterNot(_ == colName).map(s => col(s)): _*)
  }

  def groupUp(df: DataFrame, colName: String): DataFrame = {
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

  protected def getLevels(df: DataFrame, lvl: Int, max: Int): (DataFrame, DataFrame) = {
    val (h1, b1) = extractLvl(df, lvlNames(lvl), lvl)
    if (lvl < max) {
      val (h2, b2) = getLevels(b1, lvl + 1, max)
      (h1.union(h2), b2)
    } else {
      (h1, b1)
    }
  }

  def getGrammar(df: DataFrame): (DataFrame, DataFrame) = {
    val max = Math.min(gramList.map(_.precedence).max,lvlNames.length)
    val (baseGram, remainder) = getLevels(df, 1, max)

    val filteredRemainder = filterUdf.map(fc => groupUp(remainder,"name").filter(fc(col("name"),col("total")))).getOrElse(remainder)
    val toTerminusDf = groupUp(extractDF(filteredRemainder,"name",terminusMappingUDF),"name").withColumn("parent", lit(lvlNames.last))
    val otherDF = filteredRemainder.filter(remainderUDF(col("name")))

    (baseGram.union(toTerminusDf),otherDF)
  }
}

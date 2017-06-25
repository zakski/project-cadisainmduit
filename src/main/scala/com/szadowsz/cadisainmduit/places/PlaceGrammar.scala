package com.szadowsz.cadisainmduit.places

import com.szadowsz.common.io.read.CsvReader
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, sum, udf}

import scala.util.Try
import scala.util.matching.Regex

/**
  * Created on 23/06/2017.
  */
object PlaceGrammar {

  protected val placesTypes : Set[String] = getPlaces
  protected val cardinalTypes : Set[String] = Set("North","South","East","West","Northeast","Northwest","Southeast","Southwest","Northern","Southern",
    "Eastern", "Western")

  protected val placePH: String = "<<PLACENAME>>"
  protected val gramPH: String = "-->"
  protected val cardinalPH: String = "<<CARDINAL>>"
  protected val bodyPH: String = "<<BODY>>"
  protected val corePH: String = "<<CORE>>"
  protected val preTypePH: String = "<<PRETYPE>>"
  protected val postTypePH: String = "<<POSTTYPE>>"

  protected val cardinalStartRegex: Regex = ("^((?:" + cardinalTypes.mkString("|") +") )").r
  protected val cardinalEndRegex: Regex = ("( (?:" + cardinalTypes.mkString("|") +"))$").r
  protected val startRegex: Regex = ("^((?:New|Big|Little|Old|" + cardinalPH + ") ){0,1}(?:.+)$").r
  protected val bodyRegex: Regex = ("^(?:(?:New|Big|Little|Old|" + cardinalPH + ") ){0,1}(.+)$").r
  protected val typeStripRegex : Regex = ("^(.+?)(?: (?:" + placesTypes.mkString("|") + ")){0,1}$").r
  protected val postfixRegex : Regex = ("^(.+?)( (?:" + placesTypes.mkString("|") + "))$").r
//  val mountRegex = "^(Mount )(.+)$".r

  protected val startUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    s"$placePH $gramPH " + startRegex.replaceAllIn(replaceCardinalPlaceholder(name), "$1" + bodyPH)
  })

  protected val bodyRemainderUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    cardinalEndRegex.replaceAllIn(bodyRegex.replaceAllIn(replaceCardinalPlaceholder(name), "$1"),"")
  })

  protected val coreRemainderUDF : UserDefinedFunction = udf[String, String]((name: String) => {
    typeStripRegex.replaceAllIn(name, "$1")
  })

  protected val postUDF : UserDefinedFunction = udf[String, String]((name: String) => {
    val result = typeStripRegex.findFirstMatchIn(name).map{m =>
      Try{name.replaceAll(m.group(2) + "$", s" $postTypePH")}.getOrElse(name).replaceAll("^" + m.group(1), corePH)
    }
    s"$bodyPH $gramPH " + result.getOrElse(corePH)
  })

  protected val startFilterUDF : UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !name.startsWith("The ")
  })

  protected val coreFilterUDF : UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !cardinalTypes.contains(name) && !placesTypes.contains(name) && !name.split(" ").exists(n => n.length <= 1)
  })

  protected def getPlaces : Set[String] = {
    val places = new CsvReader("./archives/dict/places/placeTypes.csv")
    places.readAll().map(s => s.head.trim).toSet
  }

  protected def groupUp(df: DataFrame, colName: String): DataFrame = {
    val sums = df.columns.filterNot(_ == colName).map(c => sum(col(c)).alias(c))
    df.groupBy(colName).agg(sums.head, sums.tail: _*)
  }

  protected def replaceCardinalPlaceholder(name: String): String = {
    cardinalStartRegex.replaceAllIn(name, s"$cardinalPH ")
  }

  protected def extractDF(df: DataFrame, colName: String, func: UserDefinedFunction): DataFrame = {
    df.select(func(col(colName)).alias(colName) +: df.columns.filterNot(_ == colName).map(s => col(s)): _*)
  }

  def buildGrammar(df: DataFrame): (DataFrame,DataFrame) = {
    val start = df.filter(startFilterUDF(col("name")))
    val head = extractDF(start, "name", startUDF)
    val body = extractDF(start, "name", bodyRemainderUDF)
    val core = extractDF(body, "name", coreRemainderUDF).filter(coreFilterUDF(col("name")))
    val post = extractDF(body, "name", postUDF)
    val grammar = head.union(post)
    (grammar, grammar.union(core))
  }
}

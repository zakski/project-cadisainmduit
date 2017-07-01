package com.szadowsz.cadisainmduit.places

import com.szadowsz.cadisainmduit.places.grammar.GrBuilder
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

  protected lazy val prePlTypes: Set[String] = getPrePlaceTypes
  protected lazy val coreTypes: Map[String, String] = getCoreTypes
  protected lazy val postPlTypes: Set[String] = getPostPlaceTypes
  protected val cardinalTypes: Set[String] = Set("North", "South", "East", "West", "Northeast", "Northwest", "Southeast", "Southwest", "Northern", "Southern",
    "Eastern", "Western")

  protected val placePH: String = "<<PLACENAME>>"
  protected val gramPH: String = "-->"
  protected val cardinalPH: String = "<<CARDINAL>>"
  protected val bodyPH: String = "<<BODY>>"
  protected val corePH: String = "<<CORE>>"
  protected val animalPH: String = "<<ANIMAL>>"
  protected val bibPH: String = "<<BIB>>"
  protected val colourPH: String = "<<COLOUR>>"
  protected val empirePH: String = "<<EMPIRE>>"
  protected val eurPH: String = "<<EUR>>"
  protected val homelandPH: String = "<<HOMELAND>>"
  protected val paradisePH: String = "<<PARADISE>>"
  protected val treesPH: String = "<<PLANT>>"
  protected val otherPH: String = "<<OTHER>>"
  protected val preTypePH: String = "<<PRETYPE>>"
  protected val postTypePH: String = "<<POSTTYPE>>"

  protected val cardinalStartRegex: Regex = ("^((?:" + cardinalTypes.mkString("|") + ") )").r
  protected val cardinalEndRegex: Regex = ("( (?:" + cardinalTypes.mkString("|") + "))$").r
  protected val startRegex: Regex = ("^((?:Great|Greater|High|Low|Lower|New|Big|Little|Old|" + cardinalPH + ") ){0,1}(?:.+)$").r
  protected val bodyRegex: Regex = ("^(?:(?:Great|Greater|High|Low|Lower|New|Big|Little|Old|" + cardinalPH + ") ){0,1}(.+)$").r
  protected val typeStripRegex: Regex = ("^((?:" + prePlTypes.mkString("|") + ") ){0,1}(.+?)( (?:" + postPlTypes.mkString("|") + ")){0,1}$").r
  //  val mountRegex = "^(Mount )(.+)$".r

  protected val startUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    s"$placePH $gramPH " + startRegex.replaceAllIn(replaceCardinalPlaceholder(name), "$1" + bodyPH)
  })

  protected val bodyRemainderUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    cardinalEndRegex.replaceAllIn(bodyRegex.replaceAllIn(replaceCardinalPlaceholder(name), "$1"), "")
  })

  protected val coreRemainderUDF: UserDefinedFunction = udf[String, String] {
    case typeStripRegex(_, core, _) => core
  }

  protected val postUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    val result = name match {
      case typeStripRegex(null, _, null) => corePH
      case typeStripRegex(null, core, post) => name.replaceAll(post + "$", s" $postTypePH").replaceAll("^" + core, corePH)
      case typeStripRegex(pre, core, null) => name.replaceAll(core + "$", corePH).replaceAll("^" + pre, s"$preTypePH ")
      case typeStripRegex(pre, core, post) => name.replaceAll(post + "$", s" $postTypePH").replaceAll(core, corePH).replaceAll("^" + pre, s"$preTypePH ")
    }
    s"$bodyPH $gramPH $result"
  })

  protected val coreTypesUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    s"$corePH $gramPH ${coreTypes.getOrElse(name, otherPH)}"
  })

  protected val coreTypesMappingUDF: UserDefinedFunction = udf[String, String]((name: String) => {
    coreTypes.get(name).map(typ => s"$typ $gramPH $name").getOrElse(name)
  })

  protected val startFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !name.startsWith("The ") && !name.startsWith("El ") && !name.startsWith("La ")
  })

  protected val coreFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !cardinalTypes.contains(name) && !prePlTypes.contains(name) && !postPlTypes.contains(name) && !name.split(" ").exists(n => n.length <= 1)
  })

  protected val coreTypesFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    name.contains(gramPH)
  })

  protected val coreTypesFilterUDF2: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !name.contains(gramPH)
  })

  protected def getPrePlaceTypes: Set[String] = {
    val places = new CsvReader("./archives/dict/places/prePlaceTypes.csv")
    places.readAll().map(s => s.head.trim).toSet
  }

  protected def getCoreTypes: Map[String, String] = {
    val animal = new CsvReader("./archives/dict/places/animalTypes.csv").readAll().map(s => s.head.trim -> animalPH)
    val bib = new CsvReader("./archives/dict/places/biblicalTypes.csv").readAll().map(s => s.head.trim -> bibPH)
    val colour = new CsvReader("./archives/dict/places/colourTypes.csv").readAll().map(s => s.head.trim -> colourPH)
    val empire = new CsvReader("./archives/dict/places/empireTypes.csv").readAll().map(s => s.head.trim -> empirePH)
    val eur = new CsvReader("./archives/dict/places/eurTypes.csv").readAll().map(s => s.head.trim -> eurPH)
    val homeland = new CsvReader("./archives/dict/places/homelandTypes.csv").readAll().map(s => s.head.trim -> homelandPH)
    val paradise = new CsvReader("./archives/dict/places/paradiseTypes.csv").readAll().map(s => s.head.trim -> paradisePH)
    val trees = new CsvReader("./archives/dict/places/treeTypes.csv").readAll().map(s => s.head.trim -> treesPH)
    val list = animal ++ bib ++ colour ++ empire ++ eur ++ homeland ++ paradise ++ trees
    list.toMap
  }

  protected def getPostPlaceTypes: Set[String] = {
    val places = new CsvReader("./archives/dict/places/postPlaceTypes.csv")
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

  def buildGrammar(df: DataFrame): (DataFrame, DataFrame) = {
    val start = df.filter(startFilterUDF(col("name")))
    GrBuilder.extract(start)
//    val head = extractDF(start, "name", GrBuilder.conformUdfAtLevel(1,bodyPH))
//    val body = extractDF(start, "name", bodyRemainderUDF)
//
//    //    val core = extractDF(body, "name", coreRemainderUDF).filter(coreFilterUDF(col("name")))
//    //    val coreTypes = extractDF(core, "name", coreTypesUDF)
//    //    val coreTypesMapped = extractDF(core, "name", coreTypesMappingUDF)
//
//    //    val post = extractDF(body, "name", postUDF)
//
//    // val grammar = head.union(post).union(coreTypes).union(coreTypesMapped.filter(coreTypesFilterUDF(col("name"))))
//    //   (grammar, coreTypesMapped.filter(coreTypesFilterUDF2(col("name"))))
//    (head, body)
  }

  def getRemainder(df: DataFrame): DataFrame = {
    val start = df.filter(startFilterUDF(col("name")))
    val head = extractDF(start, "name", startUDF)
    val body = extractDF(start, "name", bodyRemainderUDF)
    extractDF(body, "name", coreRemainderUDF).filter(coreFilterUDF(col("name")))
  }
}

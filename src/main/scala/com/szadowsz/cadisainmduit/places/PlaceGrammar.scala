package com.szadowsz.cadisainmduit.places

import com.szadowsz.cadisainmduit.places.grammar.{DashSufffixGrTerm, GrBuilder, GrTerm, PrefixSufffixGrTerm}
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


  protected lazy val endTypes: Map[String, String] = getTerminusTypes
  protected lazy val postPlTypes: Set[String] = getPostPlaceTypes
  protected lazy val dashTypes: Set[String] = getPostDashTypes

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
    PrefixSufffixGrTerm("Pre_Regal", List("Royal"), true, 1),
    PrefixSufffixGrTerm("Post_Regal", List("Regis"), false, 1),
    PrefixSufffixGrTerm("Pre", List("Mount", "Camp", "Fort", "Port", "Point"), true, 2),
    PrefixSufffixGrTerm("Post", postPlTypes.toList, false, 2),
    DashSufffixGrTerm("Dash", dashTypes.toList, 2)
  )

  val lvlNames = List("<<PLACENAME>>", "<<BODY>>", "<<CORE>>")


  protected val placePH: String = "<<PLACENAME>>"
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

  protected val startFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !name.startsWith("The ") && !name.startsWith("El ") && !name.startsWith("La ")
  })
  //
  //  protected val coreFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
  //    !cardinalTypes.contains(name) && !prePlTypes.contains(name) && !postPlTypes.contains(name) && !name.split(" ").exists(n => n.length <= 1)
  //  })
  //
  //  protected val coreTypesFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
  //    name.contains(gramPH)
  //  })
  //
  //  protected val coreTypesFilterUDF2: UserDefinedFunction = udf[Boolean, String]((name: String) => {
  //    !name.contains(gramPH)
  //  })
  //
  //  protected def getPrePlaceTypes: Set[String] = {
  //    val places = new CsvReader("./archives/dict/places/prePlaceTypes.csv")
  //    places.readAll().map(s => s.head.trim).toSet
  //  }
  //
    protected def getTerminusTypes: Map[String, String] = {
     val animal = new CsvReader("./archives/dict/places/animalTypes.csv").readAll().map(s => s.head.trim -> animalPH)
//      val bib = new CsvReader("./archives/dict/places/biblicalTypes.csv").readAll().map(s => s.head.trim -> bibPH)
//      val colour = new CsvReader("./archives/dict/places/colourTypes.csv").readAll().map(s => s.head.trim -> colourPH)
      val empire = new CsvReader("./archives/dict/places/empireTypes.csv").readAll().map(s => s.head.trim -> empirePH)
//      val eur = new CsvReader("./archives/dict/places/eurTypes.csv").readAll().map(s => s.head.trim -> eurPH)
      val homeland = new CsvReader("./archives/dict/places/homelandTypes.csv").readAll().map(s => s.head.trim -> homelandPH)
      val paradise = new CsvReader("./archives/dict/places/paradiseTypes.csv").readAll().map(s => s.head.trim -> paradisePH)
      val trees = new CsvReader("./archives/dict/places/plantTypes.csv").readAll().map(s => s.head.trim -> treesPH)
      val list = homeland  ++ animal /* ++ bib ++ colour */ ++ empire ++ paradise ++ trees
      list.toMap
    }

  protected def getPostPlaceTypes: Set[String] = {
    val places = new CsvReader("./archives/dict/places/postPlaceTypes.csv")
    places.readAll().map(s => s.head.trim).toSet
  }

  protected def getPostDashTypes: Set[String] = {
    val places = new CsvReader("./archives/dict/places/dashTypes.csv")
    places.readAll().map(s => s.head.trim).toSet
  }

  def getRemainder(df: DataFrame): DataFrame = {
    val start = df.filter(startFilterUDF(col("name")))
    val gr = GrBuilder(gramList, lvlNames, None,"other",Map())
    val (h, b) = gr.getGrammar(start)
    b
  }

  def buildGrammar(df: DataFrame, pop : DataFrame): (DataFrame, DataFrame) = {
    val start = df.filter(startFilterUDF(col("name")))
    val ototal = start.select(sum(col("total"))).collect().head.getLong(0)

    val names = pop.select("name").collect().map(_.toSeq.head.toString)
    val nameFil = udf((n : String, tot : Long) => names.contains(n) || tot > 1)
    val gr = GrBuilder(gramList, lvlNames, Option(nameFil), "other", endTypes)
    val (h, b) = gr.getGrammar(start)

//    val dif = ototal - b.select(sum(col("total"))).collect().head.getLong(0)
//    val totalAdj = udf((n : Long) => if (n > 200000) n - dif else n)

    (h.select(col("parent"),col("name"), col("total")),b)
  }
}

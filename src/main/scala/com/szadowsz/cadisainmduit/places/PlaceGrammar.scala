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
    PrefixSufffixGrTerm("High", List("Upper", "High", "Higher"), true, 1),
    PrefixSufffixGrTerm("Pre_Regal", List("Royal"), true, 1),
    PrefixSufffixGrTerm("Post_Regal", List("Regis"), false, 1),
    PrefixSufffixGrTerm("Pre", List("Mount", "Camp", "Fort", "Port", "Point"), true, 2),
    PrefixSufffixGrTerm("Post", postPlTypes.toList, false, 2),
    DashSufffixGrTerm("Dash", dashTypes.toList, 2)
  )

  val lvlNames = List("<<PLACENAME>>", "<<BODY>>", "<<CORE>>")

  protected val ancientPH: String = "<<ANCIENT>>"
  protected val paradisePH: String = "<<PARADISE>>"
  protected val homelandPH1: String = "<<HOMELAND1>>"
  protected val homelandPH2: String = "<<HOMELAND2>>"
  protected val indigPH: String = "<<INDIGENOUS>>"
  protected val animalPH: String = "<<ANIMAL>>"
  protected val plantsPH: String = "<<PLANT>>"
  protected val disPH: String = "<<DISCARD>>"
  protected val otherPH: String = "<<OTHER>>"

  protected val startFilterUDF: UserDefinedFunction = udf[Boolean, String]((name: String) => {
    !name.startsWith("The ") && !name.startsWith("El ") && !name.startsWith("La ") && !name.startsWith("San ") && !name.startsWith("Santa ") &&
      !name.startsWith("Santo ")
  })

  protected def getTerminusTypes: Map[String, String] = {
    val disGeneric = new CsvReader("./archives/dict/places/toDiscard/generic.csv").readAll().map(s => s.head.trim -> disPH)
    val disForeign = new CsvReader("./archives/dict/places/toDiscard/foreign.csv").readAll().map(s => s.head.trim -> disPH)
    val disNumbers = new CsvReader("./archives/dict/places/toDiscard/numbers.csv").readAll().map(s => s.head.trim -> disPH)

    val discard = disGeneric ++ disForeign ++ disNumbers

    val kepAncient = new CsvReader("./archives/dict/places/toKeep/ancient.csv").readAll().map(s => s.head.trim -> ancientPH)
    val kepParadise = new CsvReader("./archives/dict/places/toKeep/paradise.csv").readAll().map(s => s.head.trim -> paradisePH)
    val kepHome1 = new CsvReader("./archives/dict/places/toKeep/home1.csv").readAll().map(s => s.head.trim -> homelandPH1)
    val kepHome2 = new CsvReader("./archives/dict/places/toKeep/home2.csv").readAll().map(s => s.head.trim -> homelandPH2)
    val kepIndig = new CsvReader("./archives/dict/places/toKeep/indig.csv").readAll().map(s => s.head.trim -> indigPH)
    val kepAnimal = new CsvReader("./archives/dict/places/toKeep/animals.csv").readAll().map(s => s.head.trim -> animalPH)
    val kepPlants = new CsvReader("./archives/dict/places/toKeep/plants.csv").readAll().map(s => s.head.trim -> plantsPH)
    //    val kepColon = new CsvReader("./archives/dict/places/toKeep/colonial.csv").readAll().map(s => s.head.trim -> empirePH)
    //    val kepColour = new CsvReader("./archives/dict/places/toKeep/colours.csv").readAll().map(s => s.head.trim -> colourPH)
    //    val kepGeneric = new CsvReader("./archives/dict/places/toKeep/generic.csv").readAll().map(s => s.head.trim -> genericPH)
    //    val kepHome = new CsvReader("./archives/dict/places/toKeep/home.csv").readAll().map(s => s.head.trim -> homelandPH)
    //    val kepRev = new CsvReader("./archives/dict/places/toKeep/revolutionary.csv").readAll().map(s => s.head.trim -> revPH)
    //    val kepRock = new CsvReader("./archives/dict/places/toKeep/rock.csv").readAll().map(s => s.head.trim -> rockPH)
    //    val kepSaints = new CsvReader("./archives/dict/places/toKeep/saints.csv").readAll().map(s => s.head.trim -> saintPH)

    val keep = kepAncient ++ kepParadise ++ kepHome1 ++ kepHome2 ++ kepIndig ++ kepAnimal ++ kepPlants
    //kepGeneric ++ kepRev ++ kepRock ++ kepColon ++ kepColour ++ kepForeign ++ kepHome ++ kepParadise ++ kepSaints

    val list = keep ++ discard
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
    val gr = GrBuilder(gramList, lvlNames, None, "other", Map())
    val (h, b) = gr.getGrammar(start)
    b
  }

  def buildGrammar(df: DataFrame, pop: DataFrame): (DataFrame, DataFrame) = {
    val start = df.filter(startFilterUDF(col("name")))
    val ototal = start.select(sum(col("total"))).collect().head.getLong(0)

    val names = pop.select("name").collect().map(_.toSeq.head.toString)
    val nameFil = udf((n: String, tot: Long) => names.contains(n) || tot > 1)
    val gr = GrBuilder(gramList, lvlNames, Option(nameFil), "other", endTypes)
    val (h, b) = gr.getGrammar(start)

    //    val dif = ototal - b.select(sum(col("total"))).collect().head.getLong(0)
    //    val totalAdj = udf((n : Long) => if (n > 200000) n - dif else n)

    (h.select(col("parent"), col("name"), col("total")), b)
  }
}

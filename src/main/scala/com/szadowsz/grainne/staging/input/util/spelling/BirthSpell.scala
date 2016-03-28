package com.szadowsz.grainne.input.util.spelling

import java.io.FileReader
import scala.collection.JavaConverters._

import com.szadowsz.grainne.data.County
import com.szadowsz.grainne.tools.distance.JaroWrinklerDistance
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.util.Try

/**
  * Created by zakski on 04/03/2016.
  */
object BirthSpell extends Serializable {

  /**
    * Map of recorded field spelling matches
    */
  private var countryResults: Map[String, String] = loadSpellings("./data/dict/birth/ctryList.csv")

  private var cityResults: Map[String, (String,String)] = loadCitySpellings("./data/dict/birth/cityList.csv")

  private val commonwealthResults: List[String] = loadSpellings("./data/dict/birth/cwList.csv").keys.toList

  /**
    * Map of accepted field spellings
    */
  private val countrySpellings: List[String] = countryResults.keys.toList

  private val citySpellings: List[String] = cityResults.keys.toList

  private val countySpellings: Seq[String] = County.values().map(_.toString)


  /**
    * Method to load in accepted answers and matches.
    *
    * @param directory the directory of the dictionary.
    * @return a list of accepted answers and their matches.
    */
  private def loadSpellings(directory: String): Map[String, String] = {
    var spellings = Map[String, String]()

    val csvReader = new CsvListReader(new FileReader(directory), CsvPreference.STANDARD_PREFERENCE)

    var fields: Option[Iterable[String]] = None
    do {
      fields = Option(csvReader.read()).map(_.asScala)

      fields.foreach(l => {
        val key = l.head.trim
        spellings = spellings + (key -> (if (l.size == 2) l.last else key))
      })
    } while (fields.isDefined)

    spellings
  }

  /**
    * Method to load in accepted answers and matches.
    *
    * @param directory the directory of the dictionary.
    * @return a list of accepted answers and their matches.
    */
  private def loadCitySpellings(directory: String): Map[String, (String,String)] = {
    var spellings = Map[String, (String,String)]()

    val csvReader = new CsvListReader(new FileReader(directory), CsvPreference.STANDARD_PREFERENCE)

    var fields: Option[Iterable[String]] = None
    do {
      fields = Option(csvReader.read()).map(_.asScala)

      fields.foreach(l => {
        val key = l.head.trim
        if (l.size == 2) {
          val county = parseCounty(l.last)
          spellings = spellings + (key -> (county._2,county._3))
        } else if (l.size == 3)
          spellings = spellings + (key -> ("MISSING",l.last))
      })
    } while (fields.isDefined)

    spellings
  }



  private def matchCounty(input: String): (String, String, String) = {
    input match {
      case "Londonderry" => ("", County.DERRY.toString, "Ireland")
      case q if JaroWrinklerDistance.distance(q, "Queen's") >= 0.85 => ("MISSING", County.LAOIS.toString, "Ireland")
      case k if JaroWrinklerDistance.distance(k, "King's") >= 0.85 => ("MISSING", County.OFFALY.toString, "Ireland")
      case c if Try(County.fromString(input).toString).isSuccess => ("MISSING", County.fromString(input).toString, "Ireland")
      case _ => val candidate = matchWord(input, countySpellings: _*)
        if (candidate._2 > 0.85) {
          ("MISSING", County.fromString(candidate._1).toString, "Ireland")
        } else {
          (input, "MISSING", "MISSING")
        }
    }
  }

  private def parseCounty(input: String): (String, String, String) = {
    input match {
      case c if c.startsWith("C Of ") => matchCounty(c.substring(5))
      case c if c.startsWith("C ") => matchCounty(c.substring(2))

      case of if of.startsWith("Of ") => matchCounty(of.substring(3))

      case co if co.startsWith("Co Of ") => matchCounty(co.substring(6))
      case co if co.startsWith("Co ") => matchCounty(co.substring(3))
      case co if co.endsWith(" Co") => matchCounty(co.substring(0, input.length - 3))

      case county if county.startsWith("County Of ") => matchCounty(county.substring(10))
      case county if county.startsWith("County ") => matchCounty(county.substring(7))
      case county if county.endsWith(" County") => matchCounty(county.substring(0, input.length - 7))

      case _ => matchCounty(input)
    }
  }

  /**
    * Compare individual words against each other.
    *
    * @param word    the word to find a match for.
    * @param targets the potential candidates to check.
    * @return the best candidate and the degree to which it matches.
    */
  protected def matchWord(word: String, targets: String*): (String, Double) = {
    val results = targets.map(t => (t, JaroWrinklerDistance.distance(word, t)))
    results.reduceLeft((x, y) => if (x._2 > y._2) x else y)
  }

  private def parseCity(input: String): (String, String, String) = {
    if (cityResults.contains(input)) {
      ("MISSING", cityResults(input)._1, cityResults(input)._2)
    } else {
      val candidate = matchWord(input, citySpellings.toList: _*)
      if (candidate._2 >= 0.85) {
        cityResults = cityResults + (input -> cityResults(candidate._1))
        ("MISSING", cityResults(input)._1, cityResults(input)._2)
      } else {
        (input, "MISSING","MISSING")
      }
    }
  }

  private def parseCountry(input: String): (String, String) = {
    if (countryResults.contains(input)) {
      ("MISSING", countryResults(input))
    } else {
      val candidate = matchWord(input, countrySpellings.toList: _*)
      if (candidate._2 >= 0.85) {
        countryResults = countryResults + (input -> countryResults(candidate._1))
        ("MISSING", countryResults(candidate._1))
      } else {
        (input, "MISSING")
      }
    }
  }

  def isCommonwealth(input: String): Boolean = {
    commonwealthResults.contains(input)
  }

  /**
    * Method to encode the input based on the loaded dictionary data
    *
    * @param input the input phrase.
    * @return the converted output.
    */
  def parse(input: String): (String, String, String) = {
    val (r1, ctry1) = parseCountry(input.split("\\(|\\)|,|\\.| |-").map(_.trim).filter(_.length > 0).mkString(" "))

    if (ctry1 == "MISSING") {
      val (r2, cty1, ctry2) = parseCounty(r1)
      if (ctry2 == "MISSING") {
        val (r3, cty2, ctry3) = parseCity(r1)
        (r3, cty2, ctry3)
      } else {
        (r2, cty1, ctry2)
      }
    } else {
      (r1, "MISSING", ctry1)
    }
  }
}

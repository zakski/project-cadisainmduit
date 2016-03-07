package com.szadowsz.grainne.input.util.spelling

import java.io.FileReader
import java.util

import scala.collection.JavaConverters._

import com.szadowsz.grainne.tools.distance.JaroWrinklerDistance
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

/**
  * Created by zakski on 08/01/2016.
  */
trait ColumnSpell {

  /**
    * Map of recorded individual word spelling matches
    */
  protected var wordResults: Map[String, String]

  /**
    * Map of accepted individual word spellings
    */
  protected val wordSpellings: List[String]

  /**
    * Minimum threshold for a confirmed individual word match
    */
  protected val wordViableMatch: Double

  /**
    * Map of recorded field spelling matches
    */
  protected var phraseResults: Map[String, String]

  /**
    * Map of accepted field spellings
    */
  protected val phraseSpellings: List[String]

  /**
    * Minimum threshold for a confirmed field match
    */
  protected val phraseViableMatch: Double

  protected def getReplacementWord(word : String) = {
    word
  }

  protected def getReplacementPhrase(phrase : String) = {
    "MISSING"
  }

  /**
    * Method to load in accepted answers and matches.
    *
    * @param directory the directory of the dictionary.
    * @return a list of accepted answers and their matches.
    */
  protected def loadSpellings(directory: String): Map[String, String] = {
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
    * Method to handle exceptions that would otherwise be mapped to wrong values.
    *
    * @param input the phrase to look at.
    * @return the fixed string.
    */
  protected def handleExceptions(input: String): String = input

  /**
    * Method to handle splitting the overall field value into word tokens.
    *
    * @param input the phrase to look at.
    * @return the word token sequence.
    */
  protected def handleSplitting(input: String): Seq[String] = input.split(" ")

  /**
    * Method to handle the normalisation sequence before we try to spell check each word.
    *
    * @param input the phrase to look at.
    * @return the word token sequence.
    */
  protected def handlePreProcessing(input: String): Seq[String] = {
    val normalised = handleExceptions(handleSplitting(input).mkString(" "))
    handleSplitting(normalised)
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


  /**
    * Method to handle the matching of individual word tokens.
    *
    * @param words the word token sequence.
    * @return reconstructed matched sequence.
    */
  protected def handleWordTokens(words: Seq[String]): String = {
    words.map(word => {
      if (wordResults.contains(word)) {
        wordResults(word)
      } else if (wordSpellings.nonEmpty) {
        val candidate = matchWord(word, wordSpellings: _*)
       if (candidate._2 >= wordViableMatch) {
          wordResults = wordResults + (word -> wordResults(candidate._1))
          wordResults(candidate._1)
        } else {
          wordResults = wordResults + (word -> getReplacementWord(word))
          wordResults(word)
        }
      } else {
        wordResults = wordResults + (word -> getReplacementWord(word))
        wordResults(word)
      }

    }).filter(_ != "").mkString(" ")
  }

  /**
    * Method to handle the matching of the overall phrase.
    *
    * @param normalised the string of individually mapped words.
    * @return overall matched phrase.
    */
  protected def handlePhrase(normalised: String): String = {
    if (phraseResults.contains(normalised)) {
      phraseResults.getOrElse(normalised, getReplacementPhrase(normalised))
    } else if (phraseSpellings.nonEmpty) {
        val candidate = matchWord(normalised, phraseSpellings.toList: _*)
        if (candidate._2 >= phraseViableMatch) {
          phraseResults = phraseResults + (normalised -> phraseResults(candidate._1))
          phraseResults(candidate._1)
        } else {
          phraseResults = phraseResults + (normalised -> getReplacementPhrase(normalised))
          getReplacementPhrase(normalised)
        }
      } else {
        phraseResults = phraseResults + (normalised -> getReplacementPhrase(normalised))
        getReplacementPhrase(normalised)
    }
  }

  /**
    * Method to encode the input based on the loaded dictionary data
    *
    * @param input the input phrase.
    * @return the converted output.
    */
  def parse(input: String): String = {
    val tokens = handlePreProcessing(input)
    val normalised = handleWordTokens(tokens)
    handlePhrase(normalised)
  }
}

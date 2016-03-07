package com.szadowsz.grainne.input.util.spelling

import com.szadowsz.grainne.input.util.WordFormatting

/**
  * Created by zakski on 15/11/2015.
  */
object BeliefsSpell extends ColumnSpell with Serializable{

  override protected var wordResults: Map[String, String] = loadSpellings("./data/dict/religion/relDict.csv")

  override protected val wordSpellings: List[String] = wordResults.keys.toList

  override protected var phraseResults: Map[String, String] = loadSpellings("./data/dict/religion/relList.csv")

  override protected val phraseSpellings: List[String] = phraseResults.keys.toList

  override protected val wordViableMatch: Double = 0.85

  override protected val phraseViableMatch: Double = 0.85

  /**
    * Method to handle splitting the overall field value into word tokens.
    *
    * @param input the phrase to look at.
    * @return the word token sequence.
    */
  override protected def handleSplitting(input: String): Seq[String] = {
    WordFormatting.capitalizeFully(input.trim,List(' ','-',')','(','.')).split(" ")
  }

  /**
    * Method to handle exceptions that would otherwise be mapped to wrong values.
    *
    * @param input the phrase to look at.
    * @return the fixed string
    */
  override protected def handleExceptions(input: String): String = {
    var religion = input.replaceAll("\\bR C\\b", "Roman Catholic")
    religion = religion.replaceAll("\\bU F\\b", "United Free")
    religion = religion.replaceAll("\\bG A\\b", "General Assembly")
    religion
  }

  override protected def getReplacementWord(word : String) = ""
}

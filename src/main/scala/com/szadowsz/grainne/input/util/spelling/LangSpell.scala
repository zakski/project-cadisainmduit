package com.szadowsz.grainne.input.util.spelling

import com.szadowsz.grainne.input.util.WordFormatting

/**
  * Created by zakski on 04/03/2016.
  */
object LangSpell extends ColumnSpell with Serializable{

  override protected var wordResults: Map[String, String] = loadSpellings("./data/dict/lang/langDict.csv")

  override protected val wordSpellings: List[String] = wordResults.keys.toList

  override protected var phraseResults: Map[String, String] = loadSpellings("./data/dict/lang/langList.csv")

  override protected val phraseSpellings: List[String] = phraseResults.keys.toList

  override protected val wordViableMatch: Double = 0.65

  override protected val phraseViableMatch: Double = 0.65

//  override protected def getReplacementPhrase(phrase : String) = {
//    phrase
//  }


  /**
    * Method to handle exceptions that would otherwise be mapped to wrong values.
    *
    * @param input the phrase to look at.
    * @return the fixed string.
    */
  override protected def handleSplitting(input: String): Seq[String] = {
    WordFormatting.capitalizeFully(input.split("\\(|\\)|,|\\.| |-").map(_.trim).filter(_.length > 0).mkString(" "),List(' ','-',')','(','.')).split(" ")
  }
}

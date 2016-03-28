package com.szadowsz.grainne.staging.input.util.spelling.simple

import com.szadowsz.grainne.tools.lang.WordFormatting

/**
  * Created by zakski on 04/03/2016.
  */
object LitSpell extends ColumnSpell with Serializable{

  override protected var wordResults: Map[String, String] = loadSpellings("./data/dict/literacy/litDict.csv")

  override protected val wordSpellings: List[String] = wordResults.keys.toList

  override protected var phraseResults: Map[String, String] = loadSpellings("./data/dict/literacy/litList.csv")

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

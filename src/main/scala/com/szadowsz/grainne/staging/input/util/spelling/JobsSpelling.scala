package com.szadowsz.grainne.input.util.spelling

import com.szadowsz.grainne.staging.input.util.spelling.simple.ColumnSpell

/**
  * Created by zakski on 15/11/2015.
  */
object JobsSpelling extends ColumnSpell with Serializable {

  override protected var wordResults: Map[String, String] = loadSpellings("./data/dict/job/occuDict.csv")

  override protected val wordSpellings: List[String] = wordResults.keys.toList

  override protected var phraseResults: Map[String, String] = loadSpellings("./data/dict/occupationDictionary.csv")

  override protected val phraseSpellings: List[String] = phraseResults.keys.toList

  override protected val wordViableMatch: Double = 0.85

  override protected val phraseViableMatch: Double = 0.85

  protected val invalideList = List("Retired", "Unemployed","?")
  protected val pivotList = List("In", "Of")
  protected val vanillaClassifierList = List("General")
  protected val multiWordClassifierList = List("Domestic Servant","Servant Domestic")
  protected val redundantHeadList = List("A","An")
  protected val pivotImmuneHeadList = List("Apprentice")

  /**
    * Method to handle exceptions that would otherwise be mapped to wrong values.
    *
    * @param desc the phrase to look at.
    * @return the fixed string.
    */
  override protected def handleSplitting(desc: String): Seq[String] = desc.split("\\(|\\)|,|\\.| |-").map(_.trim).filter(_.length > 0)

  protected def handlePotentialRedundantWords(words: Seq[String]): Seq[String] = if (words.indexWhere(redundantHeadList.contains) == 0){words.tail }else{ words}

  protected def handleUnwantedClassifiers(words: Seq[String]): Seq[String] = {
    if (words.length > 1){
      var tmp = words.filterNot(vanillaClassifierList.contains)
      if (tmp.length > 2) {
        var occ = tmp.mkString(" ")
        multiWordClassifierList.foreach(d => occ = occ.replaceAll(d, ""))
        tmp = handleSplitting(occ)
      }
      tmp
    } else {
      words
    }
  }

  protected def handlePotentialPivot(words: Seq[String]): Seq[String] = {
    val pivotPoints = words.count(pivotList.contains)
    if (pivotPoints == 1) {
      val head = if (pivotImmuneHeadList.contains(words.head)) List(words.head) else List()
      val tmp = if (head.nonEmpty) words.tail else words
      val parts = tmp.span(!pivotList.contains(_))
      head ++ parts._2.tail ++ parts._1
    } else {
      words
    }
  }

  override protected def handlePreProcessing(desc: String): Seq[String] = {
    var words = handleSplitting(invalideList.find(desc.contains).getOrElse(desc))

    words = checkWords(words)

    words = handlePotentialRedundantWords(words)

    words = handleUnwantedClassifiers(words)

    words = handlePotentialPivot(words)

    words = handlePotentialRedundantWords(words)


    words
  }

  protected def checkWords(words: Seq[String]): Seq[String] = {
    words.map(word => {
      if (wordResults.contains(word)) {
        wordResults(word)
      } else if (word.length > 3) {

        val candidate = matchWord(word, wordSpellings:_*)
        val result = if (candidate._2 >= wordViableMatch) {
          wordResults = wordResults + (word -> wordResults(candidate._1))
          wordResults(candidate._1)
        } else {
          wordResults = wordResults + (word -> word)
          word
        }
        result
      } else {
        word
      }
    })
  }

  override def parse(desc: String): String = {
//    val normalised = checkWords(desc)
//
//    if (phraseResults.contains(normalised)) {
//      phraseResults.getOrElse(normalised, normalised)
//    } else {
//      val candidate = matchWord(normalised, phraseSpellings.toList: _*)
//      if (candidate._2 >= phraseViableMatch) {
//        phraseResults = phraseResults + (normalised -> phraseResults(candidate._1))
//        phraseResults(candidate._1)
//      } else {
//        phraseResults = phraseResults + (normalised -> normalised)
//        normalised
//      }
//    }
    handlePreProcessing(desc).mkString(" ")
  }

}
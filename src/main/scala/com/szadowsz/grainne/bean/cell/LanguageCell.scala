package com.szadowsz.grainne.bean.cell

import com.szadowsz.grainne.bean.data.Language
import com.szadowsz.grainne.distance.JaroWrinklerDistance
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object LanguageCell {

  val negatives = List(
    "No",
    "Nil",
    "None",
    "Cannot",
    "Not",
    "Cant",
    "Nothing"
  )


  def apply(): LanguageCell = new LanguageCell()

}

/**
  * Cell to process gender and record distribution.
  *
  * @author Zakski : 29/07/2015.
  */
class LanguageCell extends CellProcessorAdaptor {

  private def matchWord(word :String, targets : String*):(String,Double)={
    val results = targets.map(t => (t,JaroWrinklerDistance.distance(word,t)))
    results.reduceLeft((x,y) => if (x._2 > y._2) x else y)
  }

  private def matchLangs(desc: String): List[Language] = {
    val langs = Language.values().map(lang => lang.text -> lang).toMap
    val words = desc.split(" ")
    val candidates = words.map(w => (matchWord(w,langs.keys.toList:_*),matchWord(w,LanguageCell.negatives:_*)))
    val results = candidates.filter( can => can._1._2 > 0.66 || can._2._2 > 0.66).map(can => if (can._1._2 > can._2._2) can._1 else can._2)
    if(results.forall(can => langs.contains(can._1))){
      results.map(_._1).sorted.map(langs(_)).toList
    } else {
      List(Language.ENGLISH)
    }
  }


  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)
    opt match {
      case Some(languages: String) => matchLangs(languages)
      case _ => List(Language.ENGLISH) // We will assume that they only speak English if no value for knowsIrish is filled in.
    }
  }
}

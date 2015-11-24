package com.szadowsz.grainne.census.supercsv.write

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
  * Created by zakski on 14/11/2015.
  */
class StringSeqCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case Some(Nil) => "MISSING"
      case Some(set : Set[Any]) => set.mkString("|")
      case Some(list : List[Any]) => list.mkString("|")
      case list : List[Any] => list.mkString("|")
      case _ => "MISSING"
    }
  }
}

package com.szadowsz.census.supercsv.read

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
  * Created by zakski on 13/11/2015.
  */
class OptionCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)
    opt match {
      case Some(s : String) => Option(s.trim)
      case _ => opt
    }
  }
}

package com.szadowsz.census.supercsv

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
  * Created by zakski on 13/11/2015.
  */
class MissingCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case s: String => s.trim
      case _ => "MISSING"
    }
  }
}

package com.szadowsz.census.supercsv.write

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
  * Created by zakski on 14/11/2015.
  */
class StringCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case Some(name : String) => name
      case _ => "MISSING"
    }
  }
}

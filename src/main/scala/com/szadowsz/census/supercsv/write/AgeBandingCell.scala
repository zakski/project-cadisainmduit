package com.szadowsz.census.supercsv.write

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
 * Cell to process gender and record distribution.
 *
 * @author Zakski : 11/11/2015.
 */
class AgeBandingCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case Some(band : (Int,Int)) => band._1 + "-" + band._2
      case _ => "MISSING"
    }
  }
}

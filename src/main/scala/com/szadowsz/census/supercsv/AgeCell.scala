package com.szadowsz.census.supercsv

import com.szadowsz.census.mapping.{AgeBanding, Gender}
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
 * Cell to process gender and record distribution.
 *
 * @author Zakski : 11/11/2015.
 */
class AgeCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case s : String => AgeBanding.ageToBand(s.toInt).toString()
      case _ => "MISSING"
    }
  }
}

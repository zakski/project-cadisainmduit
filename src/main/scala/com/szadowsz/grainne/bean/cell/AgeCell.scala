package com.szadowsz.grainne.bean.cell

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object AgeCell {

  def apply(): AgeCell = new AgeCell()
}

/**
 * Cell to process gender and record distribution.
 *
 * @author Zakski : 11/11/2015.
 */
class AgeCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    value match {
      case s : String => Some(s.toInt)
      case _ => None
    }
  }
}

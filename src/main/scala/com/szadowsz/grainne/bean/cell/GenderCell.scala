package com.szadowsz.grainne.bean.cell

import com.szadowsz.grainne.bean.data.Gender
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object GenderCell {

  def apply(): GenderCell = new GenderCell()
}

/**
 * Cell to process gender and record distribution.
 *
 * @author Zakski : 29/07/2015.
 */
class GenderCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)

    opt match {
      case Some("M") => Gender.MALE
      case Some("F") => Gender.FEMALE
      case None => Gender.MISSING
      case _ => Gender.OTHER
    }
  }
}

package com.szadowsz.census.supercsv.read

import com.szadowsz.census.mapping.Gender
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

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

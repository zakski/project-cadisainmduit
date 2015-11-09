package com.szadowsz.census.akka.process.cell

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

/**
 * Cell to process gender and record distribution.
 *
 * @author Zakski : 29/07/2015.
 */
class GenderCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val sex = value.asInstanceOf[String]

    sex match {
      case "M" => Gender.MALE
      case "F" => Gender.FEMALE
      case _ => Gender.UNKNOWN
    }
  }
}

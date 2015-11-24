package com.szadowsz.grainne.bean.cell

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object DefaultCell {

  def apply(): DefaultCell = new DefaultCell()
}

/**
  * Created by zakski on 13/11/2015.
  */
class DefaultCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)
    opt match {
      case Some(s : String) => Option(s.trim)
      case _ => opt
    }
  }
}

package com.szadowsz.grainne.bean.cell

import com.szadowsz.grainne.bean.data.County
import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.cellprocessor.ift.CellProcessor
import org.supercsv.util.CsvContext

object CountyCell {

  def apply(): CountyCell = new CountyCell()
}

/**
  * Created by Zakski on 14/11/2015.
  */
class CountyCell extends CellProcessorAdaptor {

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)

    opt match {
      case Some("Londonderry") => County.DERRY
      case Some("Queen's Co.") => County.LAOIS
      case Some("King's Co.") => County.OFFALY
      case Some(county : String) => County.fromString(county)
      case _ => County.MISSING
    }
  }
}
package com.szadowsz.grainne.bean.cell

import org.supercsv.cellprocessor.CellProcessorAdaptor
import org.supercsv.util.CsvContext

object BarrelNameCell {

  def apply(): BarrelNameCell = new BarrelNameCell()
}

/**
  * Created by zakski on 23/11/2015.
  */
class BarrelNameCell extends CellProcessorAdaptor {

  def split(field : String):List[String]={
    field.split("[ -]").toList
  }

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val opt = Option(value)
    opt match {
      case Some(s : String) => Option(split(s.trim))
      case _ => opt
    }
  }
}

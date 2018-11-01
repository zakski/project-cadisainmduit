package com.szadowz.naming.stellaris.ships

import com.szadowz.naming.stellaris.StringUtils

case class ShipList(sections : List[ShipListSection]) {

  private def convertSections = {
    sections.sortBy(_.name).map(_.toString).mkString("\n")
  }
  
  override def toString: String = {
    s"""	ship_names = {
      |${StringUtils.indent(convertSections,2)}
      |	}""".stripMargin
  }

}

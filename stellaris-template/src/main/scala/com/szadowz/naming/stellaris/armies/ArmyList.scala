package com.szadowz.naming.stellaris.armies

import com.szadowz.naming.stellaris.StringUtils

case class ArmyList(sections : List[ArmyListSection]) {

  private def convertSections = {
    sections.sortBy(_.name).map(_.toString).mkString("\n")
  }
  
  override def toString: String = {
    s"""	army_names = {
      |${StringUtils.indent(convertSections,2)}
      |	}""".stripMargin
  }

}

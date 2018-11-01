package com.szadowz.naming.stellaris.planets

import com.szadowz.naming.stellaris.StringUtils

case class PlanList(sections : List[PlanListSection]) {

  private def convertSections = {
    sections.sortBy(_.name).map(_.toString).mkString("\n")
  }
  
  override def toString: String = {
    s"""	### PLANETS
      |
      |	planet_names = {
      |${StringUtils.indent(convertSections,2)}
      |	}""".stripMargin
  }

}

package com.szadowz.naming.stellaris.characters

import com.szadowz.naming.stellaris.StringUtils

case class CharList(sections : List[CharListSection]){

  private def convertSections = {
    sections.map(_.toString).zipWithIndex.map{case (s, i) => 
      s"""names$i = {
         |${StringUtils.indent(s,1)}
         |}""".stripMargin
    }.mkString("\n")
  }
  
  override def toString: String = {
    s"""	### CHARACTERS
      |
      |	character_names = {
      |${StringUtils.indent(convertSections,2)}
      |	}""".stripMargin
  }

}

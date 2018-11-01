package com.szadowz.naming.stellaris

object StringUtils {
  
  def indent(string : String, tabCount : Int): String = {
    val tabs = "\t" * tabCount
    tabs + string.replaceAll("\n",s"\n|$tabs")
  }
}

package com.szadowsz.naming.people.results

case class SName(name : String, pop : Map[String,NamePop]) {
  
  def properCaseName : String = name.head + name.tail.toLowerCase
  
  def popularity(rank : String): NamePop = pop.getOrElse(rank,NamePop.UNUSED)
}

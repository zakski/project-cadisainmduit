package com.szadowsz.naming.people.results

case class SNameList(names : List[SName]) {
  
  def filter(popName : String, pop: NamePop): List[SName] = {
    names.filter(n => n.popularity(popName).ordinal() >= pop.ordinal())
  }
  
  def filter(popName : String, lowerPop: NamePop, upperPop:NamePop): List[SName] = {
    names.filter(n =>  
      n.popularity(popName).ordinal() >= lowerPop.ordinal() &&
      n.popularity(popName).ordinal() <= upperPop.ordinal()
    )
  }
}

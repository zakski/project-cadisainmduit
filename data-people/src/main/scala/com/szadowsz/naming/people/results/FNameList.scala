package com.szadowsz.naming.people.results

case class FNameList(names : List[FName]) {
  
  def filter(gender : Char, popName : String, pop: NamePop): List[FName] = {
    names.filter(n => n.gender == gender && n.popularity(popName).ordinal() >= pop.ordinal())
  }
  
  def filter(gender : Char, popName : String, lowerPop: NamePop, upperPop:NamePop): List[FName] = {
    names.filter(n => n.gender == gender && 
      n.popularity(popName).ordinal() >= lowerPop.ordinal() &&
      n.popularity(popName).ordinal() <= upperPop.ordinal()
    )
  }
}

package com.szadowsz.naming.people.results

case class FNameList(names : List[BabyName]) {
  
  def filter(gender : Char, popName : String, pop: NamePop): List[BabyName] = {
    names.filter(n => n.gender == gender && n.popularity(popName).ordinal() >= pop.ordinal())
  }
  
  def filter(gender : Char, popName : String, lowerPop: NamePop, upperPop:NamePop): List[BabyName] = {
    names.filter(n => n.gender == gender && 
      n.popularity(popName).ordinal() >= lowerPop.ordinal() &&
      n.popularity(popName).ordinal() <= upperPop.ordinal()
    )
  }
}

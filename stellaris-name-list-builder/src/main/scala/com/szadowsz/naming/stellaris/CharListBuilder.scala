package com.szadowsz.naming.stellaris

import com.szadowsz.naming.people.results.reader.FNameReader
import com.szadowsz.naming.people.results.NamePop
import com.szadowz.naming.stellaris.characters.{CharList, CharListSection}

class CharListBuilder {
  
  private var filePath: String = _
  
  private var rankList: List[String] = List()
  
  private var weightList: List[Int] = List()
  
  private var popList: List[(NamePop, NamePop)] = List()
  
  def setFilePath(value: String): CharListBuilder = {
    filePath = value
    this
  }
  
  def setRankList(value: String*): CharListBuilder = {
    rankList = value.toList
    this
  }
  
  def setWeightList(value: Int*): CharListBuilder = {
    weightList = value.toList
    this
  }
  
  def setPopList(value: (NamePop, NamePop)*): CharListBuilder = {
    popList = value.toList
    this
  }
  
  def build(): CharList = {
    assert(filePath != null)
    assert(rankList.nonEmpty)
    assert(rankList.length == weightList.length)
    assert(weightList.length == popList.length)
    
    val reader = FNameReader(filePath)
    val data = reader.read
    
    CharList(for (i <- rankList.indices) yield {
      val (lower, upper) = popList(i)
      CharListSection(
        weightList(i),
        data.filter('M', rankList(i), lower, upper).map(_.properCaseName),
        data.filter('F', rankList(i), lower, upper).map(_.properCaseName),
        List(),
        List(),
        List(),
        List()
      )
    }
    )
  }
}

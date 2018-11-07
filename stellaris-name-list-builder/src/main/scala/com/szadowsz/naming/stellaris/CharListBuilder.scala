package com.szadowsz.naming.stellaris

import com.szadowsz.naming.people.results.reader.{FNameReader, SNameReader}
import com.szadowsz.naming.people.results.NamePop
import com.szadowz.naming.stellaris.characters.{CharList, CharListSection}

class CharListBuilder {
  
  private var forenameFilePath: String = _
  private var surnameFilePath: String = _
  private var regalForenameFilePath: String = _
  private var regalSurnameFilePath: String = _
  
  private var frankList: List[String] = List()
  private var srankList: List[String] = List()
  private var fRegalRankList: List[String] = List()
  private var sRegalRankList: List[String] = List()
  
  private var weightList: List[Int] = List()
  
  private var fpopList: List[(NamePop, NamePop)] = List()
  private var spopList: List[(NamePop, NamePop)] = List()
  private var fRegalPopList: List[(NamePop, NamePop)] = List()
  private var sRegalPopList: List[(NamePop, NamePop)] = List()
  
  def setForenameFilePath(value: String): CharListBuilder = {
    forenameFilePath = value
    this
  }
  
  def setSurnameFilePath(value: String): CharListBuilder = {
    surnameFilePath = value
    this
  }
  
  def setRegalForenameFilePath(value: String): CharListBuilder = {
    regalForenameFilePath = value
    this
  }
  
  def setRegalSurnameFilePath(value: String): CharListBuilder = {
    regalSurnameFilePath = value
    this
  }
  
  def setForenameRankList(value: String*): CharListBuilder = {
    frankList = value.toList
    this
  }
 
  def setSurnameRankList(value: String*): CharListBuilder = {
    srankList = value.toList
    this
  }
  
  def setRegalForenameRankList(value: String*): CharListBuilder = {
    fRegalRankList = value.toList
    this
  }
  
  def setRegalSurnameRankList(value: String*): CharListBuilder = {
    sRegalRankList = value.toList
    this
  }
  
  def setWeightList(value: Int*): CharListBuilder = {
    weightList = value.toList
    this
  }
  
  def setForenamePopList(value: (NamePop, NamePop)*): CharListBuilder = {
    fpopList = value.toList
    this
  }
 
  def setSurnamePopList(value: (NamePop, NamePop)*): CharListBuilder = {
    spopList = value.toList
    this
  }
  
  def build(): CharList = {
    assert(forenameFilePath != null)
    assert(surnameFilePath != null)
    assert(regalForenameFilePath != null)
    assert(regalSurnameFilePath != null)
    assert(frankList.nonEmpty)
    assert(frankList.length == srankList.length)
    assert(srankList.length == weightList.length)
    assert(weightList.length == fpopList.length)
    assert(fpopList.length == spopList.length)
//    assert(spopList.length == fRegalRankList.length)
//    assert(fRegalRankList.length == sRegalRankList.length)
//    assert(sRegalRankList.length == fRegalPopList.length)
//    assert(fRegalPopList.length == sRegalPopList.length)
  
    val freader = FNameReader(forenameFilePath)
    val fRegalReader = FNameReader(regalForenameFilePath)
    val sreader = SNameReader(surnameFilePath)
    val sRegalReader = SNameReader(regalSurnameFilePath)
   
    val fdata = freader.read
    val fRegalData = fRegalReader.read
    val sdata = sreader.read
    val sRegalData = sRegalReader.read
  
    CharList(for (i <- frankList.indices) yield {
      val (flower, fupper) = fpopList(i)
      val (slower, supper) = spopList(i)
      CharListSection(
        weightList(i),
        fdata.filter('M', frankList(i), flower, fupper).map(_.properCaseName),
        fdata.filter('F', frankList(i), flower, fupper).map(_.properCaseName),
        sdata.filter(srankList(i), slower, supper).map(_.properCaseName),
        fRegalData.filter('M',"NULL",NamePop.UNUSED,NamePop.BASIC).map(_.properCaseName),
        fRegalData.filter('F',"NULL",NamePop.UNUSED,NamePop.BASIC).map(_.properCaseName),
        sRegalData.filter("NULL", NamePop.UNUSED,NamePop.BASIC).map(_.properCaseName)
      )
    }
    )
  }
}

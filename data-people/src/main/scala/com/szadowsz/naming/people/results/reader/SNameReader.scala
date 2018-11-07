package com.szadowsz.naming.people.results.reader

import java.io.FileReader

import com.szadowsz.naming.people.results.{SName, SNameList, NamePop}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._
import scala.util.Try

case class SNameReader(filePath : String) {
  private val pref = new CsvPreference.Builder('"',',', "\r\n").build
  
  private def readFile: (List[String], List[List[String]]) = {
    val csv = new CsvListReader(new FileReader(filePath), pref)
    val result = Try{
      (csv.getHeader(true).toList,Iterator.continually(csv.read()).takeWhile(_ != null).toList.map(_.asScala.toList))
    }
    csv.close()
    result.getOrElse((List(),List()))
  }
  
  def read: SNameList = {
    val (headers,data) = readFile
    val nameIndex = headers.indexOf("name")
    val rankIndexs = headers.zipWithIndex.filter{case (h,i) => h.endsWith("_Rank")}.map{case (h,i) => (h.substring(0,h.lastIndexOf("_")),i)}
    SNameList(data.map(row => SName(row(nameIndex),rankIndexs.map{case (h,i) => h -> NamePop.typeOf(row(i).toLowerCase)}.toMap)))
  }
}

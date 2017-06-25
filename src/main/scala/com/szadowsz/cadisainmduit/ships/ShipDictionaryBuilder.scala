package com.szadowsz.cadisainmduit.ships

import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.common.io.write.CsvWriter
import net.sf.extjwnl.data.PointerUtils
import net.sf.extjwnl.dictionary.Dictionary
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created on 05/06/2017.
  */
object ShipDictionaryBuilder {

  val dictionary = Dictionary.getDefaultResourceInstance()

  def getDirectHypernyms(s: String): Array[String] = {
    val indexes = Try(dictionary.lookupAllIndexWords(s).getIndexWordArray)
    val senses = indexes.map(_.flatMap(iw => iw.getSenses.asScala))
    val hypernyms = senses.map(_.map(s => PointerUtils.getDirectHypernyms(s)))
    val pointers = hypernyms.map(_.flatMap(list => list.iterator().asScala.toList))
    val words = pointers.map(_.flatMap(node => node.getSynset.getWords.asScala.map(_.getLemma)).distinct)
    val results = words.getOrElse(Array())
    results
  }

  def getSynonyms(s: String): Array[String] = {
    val indexes = Try(dictionary.lookupAllIndexWords(s).getIndexWordArray)
    val senses = indexes.map(_.flatMap(iw => iw.getSenses.asScala))
    val hypernyms = senses.map(_.map(s => PointerUtils.getSynonyms(s)))
    val pointers = hypernyms.map(_.flatMap(list => list.iterator().asScala.toList))
    val words = pointers.map(_.flatMap(node => node.getSynset.getWords.asScala.map(_.getLemma)).distinct)
    val results = words.getOrElse(Array())
    results
  }

  def getHypernyms(s: String): Array[String] = {
    val indexes = Try(dictionary.lookupAllIndexWords(s).getIndexWordArray)
    val senses = indexes.map(_.flatMap(iw => iw.getSenses.asScala))
    val hypernyms = senses.map(_.map(s => PointerUtils.getHypernymTree(s)))
    val pointers = hypernyms.map(_.flatMap(tree => tree.toList.asScala.flatMap(_.iterator().asScala)))
    val words = pointers.map(_.flatMap(node => node.getSynset.getWords.asScala.map(_.getLemma)).distinct)
    val results = words.getOrElse(Array())
    results
  }

  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val words = new CsvReader("./data/web/ships.csv").readAll().map(_.head).toArray
      .flatMap(dictionary.lookupAllIndexWords(_).getIndexWordArray.map(_.getLemma)).distinct.filter(_.forall(_.isLower))

    var wordSet = words.toSet

    var batch : Array[String] = words
    var count = 0

    while (count != wordSet.size) {
      count = wordSet.size
      batch = (batch.flatMap(getSynonyms) ++ batch.flatMap(getHypernyms)).distinct.filterNot(wordSet.contains).filter(_.forall(_.isLower))
      wordSet = wordSet ++ batch
      println(count + " vs " + wordSet.size + "(" + batch.length + ")")
    }

    val writer = new CsvWriter("./data/dict/dict.csv", "UTF-8", false)
    writer.writeAll(wordSet.toList.sorted.map(List(_)))
    writer.close()
  }
}

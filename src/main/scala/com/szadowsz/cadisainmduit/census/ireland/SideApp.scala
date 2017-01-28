package com.szadowsz.cadisainmduit.census.ireland

import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.CsvReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.lang.WordTokeniser
import net.sf.extjwnl.dictionary.Dictionary

/**
  * Created on 28/11/2016.
  */
object SideApp {

  val stopWords = Set(
    "about",
    "above",
    "admitting",
    "an",
    "and",
    "anglo",
    "anything",
    "attends",
    "because",
    "belongs",
    "borgian",
    "bros",
    "brought",
    "called",
    "cannot",
    "ch",
    "children",
    "christadelphian",
    "christians",
    "converted",
    "convenanter",
    "creedless",
    "declines",
    "denom",
    "designated",
    "disestablished",
    "don't",
    "etc.",
    "evangelic",
    "for",
    "freed",
    "friends",
    "gods",
    "goes",
    "his",
    "ideas",
    "indaism",
    "inparticular",
    "is",
    "loves",
    "meets",
    "met",
    "musalsnan",
    "nor",
    "norweyian",
    "of",
    "our",
    "orangism",
    "persuasions",
    "pres",
    "persuasions",
    "protestants",
    "quakeress",
    "receiving",
    "refused",
    "religions",
    "regenerated",
    "remonstrant",
    "rewarded",
    "saints",
    "salvationist",
    "seceder",
    "seceding",
    "seventhday",
    "subscribing",
    "swedenborgian",
    "the",
    "this",
    "those",
    "to",
    "waldensian",
    "was",
    "whenever",
    "which",
    "whose",
    "with",
    "without",
    "worshipping",
    "zwinglian"
  )

  val phonetics = Map("lit" -> Set(
    "Rite",
    "Wright",
    "Right",
    "Reed",
    "Writ",
    "Red",
    "Reid",
    "Kite",
    "Am",
    "Amd",
    "An"
  ))

  def main(args: Array[String]): Unit = {
    val dict = Dictionary.getDefaultResourceInstance
    val files = FileFinder.search("./data/debug/side/1901-9", Option(new ExtensionFilter(".csv", false))).filter(_.getName.endsWith("Cap.csv"))
    files.foreach { file =>
      val reader = new CsvReader(file.getAbsolutePath)
      val words = reader.readAll().flatMap(row => WordTokeniser.tokenise(row.head)).distinct.sorted
      val name = file.getName.replaceAll("Cap.csv", "")
      val phoneticFixes = phonetics.getOrElse(name, Set())

      val fil = words.filter { s =>
        val x = dict.lookupAllIndexWords(s)
        val b = ((x.size() > 0 && x.getIndexWordArray.exists(s.toLowerCase == _.getLemma)) || stopWords.contains(s.toLowerCase())) && !phoneticFixes.contains(s)
        b
      }.map(Seq(_))

      val w = new CsvWriter(s"./data/dict/$name.csv", "UTF-8", false)
      w.writeAll(fil)
      w.close()
    }
  }
}

package com.szadowsz.naming.stellaris

import com.szadowsz.naming.people.results.reader.SNameReader
import com.szadowsz.naming.people.results.NamePop

object Test {
  
  def main(args: Array[String]): Unit = {
    val charListBuilder = new CharListBuilder()
    charListBuilder
      .setForenameFilePath("./data/results/baby_names.csv")
      .setSurnameFilePath("./data/tmp/USA/surnames.csv")
      .setRegalForenameFilePath("./archives/data/web/us/presidents_forenames.csv")
      .setRegalSurnameFilePath("./archives/data/web/us/presidents_surnames.csv")
      .setForenameRankList("NI","NI")
      .setSurnameRankList("USA","USA")
      .setWeightList(60,40)
      .setForenamePopList((NamePop.BASIC,NamePop.BASIC),(NamePop.COMMON,NamePop.COMMON))
      .setSurnamePopList((NamePop.COMMON,NamePop.BASIC),(NamePop.COMMON,NamePop.BASIC))
    println(charListBuilder.build())
  }
}

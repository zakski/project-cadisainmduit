package com.szadowsz.naming.stellaris

import com.szadowsz.naming.people.results.reader.FNameReader
import com.szadowsz.naming.people.results.NamePop

object Test {
  
  def main(args: Array[String]): Unit = {
    val charListBuilder = new CharListBuilder()
    charListBuilder
      .setFilePath("./data/results/baby_names.csv")
      .setRankList("NI","NI")
      .setWeightList(60,40)
      .setPopList((NamePop.BASIC,NamePop.BASIC),(NamePop.COMMON,NamePop.COMMON))
    println(charListBuilder.build())
  }
}

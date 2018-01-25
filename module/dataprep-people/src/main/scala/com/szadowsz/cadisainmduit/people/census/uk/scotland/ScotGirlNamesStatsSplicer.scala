package com.szadowsz.cadisainmduit.people.census.uk.scotland

import com.szadowsz.cadisainmduit.people.census.SegGenderCensusHandler
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Built to stitch all england and wales popular name data together
  *
  * Created on 19/10/2016.
  */
object ScotGirlNamesStatsSplicer extends SegGenderCensusHandler {

  protected override def getInitialCols(country: String, year: String): Array[String] = {
    Array(s"${country}rank_$year", "name", s"${country}_count_$year")
  }

  override def loadData(save: Boolean): DataFrame = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val girls = loadGirlsData("SC","./data/data/census/scotland/common/scotfirstnames-girls",sess)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
      writeDF(girls, s"./data/tmp/SC/baby_girl_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }
    girls
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


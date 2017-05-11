package com.szadowsz.cadisainmduit.census.canada

import com.szadowsz.cadisainmduit.census.SegGenderCensusHandler
import org.apache.spark.sql.DataFrame

object AlbertaNamesStatsSplicer extends SegGenderCensusHandler{

  override def loadData(save: Boolean): DataFrame = loadData("AB","./data/census/canada/alberta","./archives/data/census/canada/alberta_firstnames",save)

  override protected def getInitialCols(region: String, year: String): Array[String] = {
    Array(s"${region}_count_$year","name")
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


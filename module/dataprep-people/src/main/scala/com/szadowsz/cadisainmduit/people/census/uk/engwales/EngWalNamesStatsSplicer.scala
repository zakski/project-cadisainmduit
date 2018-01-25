package com.szadowsz.cadisainmduit.people.census.uk.engwales

import com.szadowsz.cadisainmduit.people.census.SegGenderCensusHandler
import org.apache.spark.sql.DataFrame

/**
  * Built to stitch all england and wales popular name data together
  *
  * Created on 19/10/2016.
  */
object EngWalNamesStatsSplicer extends SegGenderCensusHandler {

  protected override def selectStdCols(country: String, year: String, tmp: DataFrame): DataFrame = {
    if (year.toInt < 1996)
      tmp.select("name", s"${country}_rank_$year")
    else
      tmp.select("name", s"${country}_count_$year")
  }

  protected override def getInitialCols(country: String, year: String): Array[String] = {
    if (year.toInt < 1996)
      Array(s"${country}_rank_$year", "name")
    else
      Array(s"${country}_rank_$year", "name", s"${country}_count_$year")
  }

  override def loadData(save : Boolean) =  {
    loadData(save,"EW","./data/data/census/engwales/common/engwalesfirstnames-boys","./data/data/census/engwales/common/engwalesfirstnames-girls")
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


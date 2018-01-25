package com.szadowsz.cadisainmduit.people.census.uk.norire

import java.io.File

import com.szadowsz.cadisainmduit.people.census.SegGenderCensusHandler
import org.apache.spark.sql.DataFrame

/**
  * Built to stitch all england and wales popular name data together
  *
  * Created on 19/10/2016.
  */
object NorireNamesStatsSplicer extends SegGenderCensusHandler{

  override def loadData(save: Boolean): DataFrame = loadData(save,"NI","./data/data/census/norire/common/norirefirstnames-boys/","./data/data/census/norire/common/norirefirstnames-girls/")

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


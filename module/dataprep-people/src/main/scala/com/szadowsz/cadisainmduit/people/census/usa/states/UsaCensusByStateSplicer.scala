package com.szadowsz.cadisainmduit.people.census.usa.states

import java.io.File

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.zip.ZipperUtil
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaCensusByStateSplicer extends CensusHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def loadData(save: Boolean): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()
    UsaRegionUtil.aggStateData(save,sess,"./data/data/census/us/namesByState","./data/data/census/us/states",Nil)
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

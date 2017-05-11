package com.szadowsz.cadisainmduit.census.usa.states

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
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
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
    ZipperUtil.unzip(s"./archives/data/census/us/namesByState.zip", "./data/census/us/namesByState")

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()
    UsaRegionUtil.aggStateData(save,sess,"./data/census/us/namesByState","./data/census/us/states",Nil)
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

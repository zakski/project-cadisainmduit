package com.szadowsz.cadisainmduit.census.usa.regions

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
import com.szadowsz.cadisainmduit.census.usa.states.UsaRegionUtil
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.zip.ZipperUtil
import org.apache.spark.sql._
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaRegionStatsCreator extends CensusHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val ver = List("VT")
  val pac = List("CA", "OR", "WA", "NV", "AK", "HI")
  val front = List("MT", "WY", "ID", "UT", "CO", "AZ")
  val mAmer = List("ND", "SD", "NE", "KS", "MI", "OH", "IN", "IL", "WI", "MN", "IA", "MO")
  val tx = List("TX", "NM", "OK", "LA", "AR", "MS", "AL", "FL", "TN", "KY")
  val thrtn = List("DE", "SC", "PA", "NJ", "GA", "CT", "MA", "ME", "MD", "DC", "NH", "VA", "NY", "NC", "RI", "WV")

  def loadData(save: Boolean): DataFrame = {
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
    ZipperUtil.unzip(s"./archives/data/census/us/namesByState.zip", "./data/census/us/namesByState")

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val any = UsaRegionUtil.aggRegData(save, sess, "./data/census/us/namesByState", "./data/census/us/states", "13", thrtn)
    // val any = UsaRegionUtil.aggRegData(save,sess,"./data/census/us/namesByState","./data/census/us/states","BS", mAmer)
    // val any = UsaRegionUtil.aggRegData(save,sess,"./data/census/us/namesByState","./data/census/us/states","FS", front)
    // val any = UsaRegionUtil.aggRegData(save,sess,"./data/census/us/namesByState","./data/census/us/states","TE", tx)
    // val any = UsaRegionUtil.aggRegData(save,sess,"./data/census/us/namesByState","./data/census/us/states","PC", pac)
    // val any = UsaRegionUtil.aggRegData(save,sess,"./data/census/us/namesByState","./data/census/us/states","VR", ver)
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
    any
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

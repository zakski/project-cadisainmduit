package com.szadowsz.cadisainmduit.places.nga

import java.io.File

import com.szadowsz.cadisainmduit.places.nga.commonwealth.BaseNGACommonwealthPreparer
import com.szadowsz.cadisainmduit.places.opengov.OGPlacePreparer
import org.apache.spark.sql.{DataFrame, SparkSession}

object AmericasPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/americas","./data/results/am_placenames.csv", f => !f.getAbsolutePath.contains("Usa"))
  }

  override protected def loadData(dataPath: String, customFilter: File => Boolean, sess: SparkSession): (Array[String], DataFrame) = {
    val (codes,dfs) = super.loadData(dataPath, customFilter, sess)
    (codes :+ "us", dfs.union(USAPreparer.getUSA(sess)))
  }
}

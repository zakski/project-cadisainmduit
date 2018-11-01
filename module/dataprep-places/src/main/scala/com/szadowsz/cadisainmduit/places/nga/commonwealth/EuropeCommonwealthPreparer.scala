package com.szadowsz.cadisainmduit.places.nga.commonwealth
import java.io.File

import com.szadowsz.cadisainmduit.places.opengov.OGPlacePreparer
import org.apache.spark.sql.{DataFrame, SparkSession}

object EuropeCommonwealthPreparer extends BaseNGACommonwealthPreparer {

  def main(args: Array[String]): Unit = {
    transform("./data/data/nga/europe","./data/results/eur_placenames.csv", f => !f.getAbsolutePath.contains("United Kingdom"))
  }

  override protected def loadData(dataPath: String, customFilter: File => Boolean, sess: SparkSession): (Array[String], DataFrame) = {
    val (codes,dfs) = super.loadData(dataPath, customFilter, sess)
    (codes :+ "uk", dfs.union(OGPlacePreparer.getUK(sess)))
  }
}

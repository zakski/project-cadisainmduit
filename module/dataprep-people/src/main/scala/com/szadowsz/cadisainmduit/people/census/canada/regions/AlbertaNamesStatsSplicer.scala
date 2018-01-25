package com.szadowsz.cadisainmduit.people.census.canada.regions

import com.szadowsz.cadisainmduit.people.census.SegGenderCensusHandler
import org.apache.spark.sql.DataFrame

/**
  * Loader for Alberta specific Canadian Data.
  *
  * Data Preparation using the Segregated Gender Census Handler
  */
object AlbertaNamesStatsSplicer extends SegGenderCensusHandler{

  /**
    * Loads and Prepares the Alberta Dataset
    *
    * @param save whether to dump the data to a tmp folder
    * @return the prepared dataset
    */
  override def loadData(save: Boolean): DataFrame = {
    loadData(save,"AB","./data/data/census/canada/alberta_firstnames-boys","./data/data/census/canada/alberta_firstnames-girls")
  }

  /**
    * Method to get the initial columns
    *
    * @param region the region being used
    * @param year the year of the data
    * @return array of columns
    */
  override protected def getInitialCols(region: String, year: String): Array[String] = {
    Array(s"${region}_count_$year","name")
  }

  /**
    * Standalone Entrypoint to prep Alberta Dataset
    *
    * @param args program arguments (not used).
    */
  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


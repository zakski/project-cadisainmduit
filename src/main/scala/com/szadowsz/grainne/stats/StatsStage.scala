package com.szadowsz.grainne.stats

import com.szadowsz.grainne.data.CensusDataBean
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by zakski on 01/12/2015.
  */
class StatsStage(val beans: RDD[CensusDataBean])extends Serializable{
  private val _logger = LoggerFactory.getLogger(classOf[StatsStage])

  def execute(sc: SparkContext): Unit = {
    val start = System.currentTimeMillis()
    _logger.info("Calculating Column Statistics")

    val stats = BeanStats.calculateStats(beans)
    BeanStats.writeHighToLow(stats)

    val elapsed = System.currentTimeMillis() - start
    _logger.info("Wrote Statistics for {} beans in {} s", beans.count(), elapsed.toDouble / 1000.0)
  }

  def output: RDD[CensusDataBean] = beans
}

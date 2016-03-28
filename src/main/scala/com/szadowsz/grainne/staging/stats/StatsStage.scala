package com.szadowsz.grainne.stats

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by zakski on 01/12/2015.
  */
class StatsStage[T <: Serializable](folder : String, val beans: RDD[T])extends Serializable{
  private val _logger = LoggerFactory.getLogger(classOf[StatsStage[T]])

  def execute(sc: SparkContext): Unit = {
    val start = System.currentTimeMillis()
    _logger.info("Calculating Column Statistics")

    val stats = BeanStats.calculateStats(beans)
    BeanStats.writeHighToLow(folder,stats)

    val elapsed = System.currentTimeMillis() - start
    _logger.info("Wrote Statistics for {} beans in {} s", beans.count(), elapsed.toDouble / 1000.0)
  }
}

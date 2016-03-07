package com.szadowsz.grainne

import com.szadowsz.grainne.input.cell.{Header, SparkCell}
import com.szadowsz.grainne.input.LoadStage
import com.szadowsz.grainne.stats.StatsStage
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
  * Created by zakski on 27/11/2015.
  */
object StatsApp extends App {
  private val _logger = LoggerFactory.getLogger(StatsApp.getClass)
  val start = System.currentTimeMillis()

  val conf = new SparkConf().setMaster("local[4]").setAppName("palette-cleanse").set("spark.executor.memory", "4g")
  val sc: SparkContext = new SparkContext(conf)
  _logger.info("Configuration Initialised")

  val load = new LoadStage("./data/census/1901/", Header.getHeadersAsString, Header.getHeaders.map(SparkCell(_)), false)
  load.execute(sc)
  val stats = new StatsStage(load.output)
  stats.execute(sc)

  val elapsed = System.currentTimeMillis() - start
  _logger.info("Total Time is {} s", elapsed.toDouble / 1000.0)
}
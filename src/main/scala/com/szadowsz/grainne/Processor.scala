package com.szadowsz.grainne

import com.szadowsz.grainne.input.cell.{Header, SparkCell}
import com.szadowsz.grainne.input.LoadStage
import com.szadowsz.grainne.staging.validation.ValidationStage
import com.szadowsz.grainne.stats.StatsStage
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by zakski on 27/11/2015.
  */
object Processor {
  private val _logger = LoggerFactory.getLogger(Processor.getClass)

 def main(args : Array[String]) : Unit = {
    val start = System.currentTimeMillis()

    val conf = new SparkConf().setMaster("local[4]").setAppName("filter").set("spark.executor.memory", "4g")
    val sc: SparkContext = new SparkContext(conf)
    _logger.info("Configuration Initialised")

    val load = new LoadStage("./data/census/1901/", Header.getHeadersAsString, Header.getHeaders.map(SparkCell(_)), false)
    load.execute(sc)

//   var stats = new StatsStage("./data/raw_stats/",load.output)
//   stats.execute(sc)

   val filter = new ValidationStage(load.output)
   filter.execute(sc)

   val stats = new StatsStage("./data/filtered_stats/",filter.output)
   stats.execute(sc)


   val elapsed = System.currentTimeMillis() - start
    _logger.info("Total Time is {} s", elapsed.toDouble / 1000.0)
  }
}
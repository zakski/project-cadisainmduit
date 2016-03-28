package com.szadowsz.grainne.input

import com.szadowsz.grainne.data.CensusDataBean
import com.szadowsz.grainne.input.cell.CensusReader
import com.szadowsz.grainne.tools.io.{FileFinder, ExtensionFilter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.supercsv.cellprocessor.ift.CellProcessor

/**
  * Created by zakski on 01/12/2015.
  */
class LoadStage(val dir : String,val headers : Array[String],val cells : Array[CellProcessor],val recursive : Boolean)extends Serializable{
  private val _logger = LoggerFactory.getLogger(classOf[LoadStage])

  var beans: RDD[CensusDataBean] = null

  def execute(sc: SparkContext): Unit = {
    val start = System.currentTimeMillis()
    _logger.info("Loading Census Beans")

    val filter = ExtensionFilter(".csv", recursive)
    val data = sc.textFile(FileFinder.search(dir, filter).toList.map(_.getAbsolutePath).mkString(","))

    beans = data.map(s => {
      val reader = CensusReader(headers, cells)
      reader.read(s)
    })

    beans.persist(StorageLevel.MEMORY_AND_DISK)

    val elapsed = System.currentTimeMillis() - start
    _logger.info("Loaded {} beans in {} s", beans.count(), elapsed.toDouble / 1000.0)
  }

  def output: RDD[CensusDataBean] = beans
}

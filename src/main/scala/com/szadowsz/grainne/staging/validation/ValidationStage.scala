package com.szadowsz.grainne.staging.validation

import com.szadowsz.grainne.data.CensusDataBean
import com.szadowsz.grainne.input.cell.CensusReader
import com.szadowsz.grainne.validation.{AbstractValidator, AgeValidator}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import org.supercsv.cellprocessor.ift.CellProcessor

object ValidationStage {

  val default = List[AbstractValidator[CensusDataBean]](
    new AgeValidator,
    new GenderValidator,
    new NationalityValidator,
    new LanguageValidator,
    new NameValidator
  )
}

/**
  * Created by zakski on 01/12/2015.
  */
class ValidationStage(val input : RDD[CensusDataBean], val validators : List[AbstractValidator[CensusDataBean]] = ValidationStage.default) extends Serializable{
  private val _logger = LoggerFactory.getLogger(classOf[ValidationStage])

  var beans: RDD[CensusDataBean] = null

  def execute(sc: SparkContext): Unit = {
    val start = System.currentTimeMillis()
    _logger.info("Filtering Census Beans")

    beans = input.filter(b => validators.forall(_.validate(b)))
    beans.persist(StorageLevel.MEMORY_AND_DISK)

    val elapsed = System.currentTimeMillis() - start
    _logger.info("Filtered {} beans in {} s", beans.count(), elapsed.toDouble / 1000.0)
  }

  def output: RDD[CensusDataBean] = beans
}

package com.szadowsz.cadisainmduit.census.uk.scotland

import java.io.File

import com.szadowsz.cadisainmduit.census.uk.CountryNameStatsSplicer
import com.szadowsz.cadisainmduit.census.uk.norire.NorireNamesStatsSplicer._
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.DataFrameNaFunctions
import org.slf4j.LoggerFactory

/**
  * Built to stitch all england and wales popular name data together
  *
  * Created on 19/10/2016.
  */
object ScotNamesStatsSplicer extends CountryNameStatsSplicer {

  protected override def getInitialCols(country: String, year: String): Array[String] = {
    Array(s"${country}rank_$year", "name", s"${country}_count_$year")
  }

  override def loadData(save: Boolean): DataFrame = loadData("SC","./data/census/scot/","./archives/data/census/scotland/common/scotfirstnames",save)

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


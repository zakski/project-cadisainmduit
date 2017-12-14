package com.szadowsz.cadisainmduit.places

import java.io.File

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.{Lineage, LocalDataframeIO}
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.spelling.RegexValidationTransformer
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created on 27/04/2016.
  */
object USAPreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

  val usaSchema = Array("FEATURE_ID", "FEATURE_NAME", "FEATURE_CLASS", "STATE_ALPHA", "STATE_NUMERIC", "COUNTY_NAME", "COUNTY_NUMERIC", "PRIMARY_LAT_DMS",
    "PRIM_LONG_DMS", "PRIM_LAT_DEC", "PRIM_LONG_DEC ", "SOURCE_LAT_DMS", "SOURCE_LONG_DMS", "SOURCE_LAT_DEC", "SOURCE_LONG_DEC", "ELEV_IN_M", "ELEV_IN_FT", "MAP_NAME",
    "DATE_CREATED", "DATE_EDITED")

  val dropList = List("UFI", "UNI", "LAT", "LONG", "MGRS", "JOG", "DSG", "PC", "ADM1", "POP", "ELEV", "CC2", "MODIFY_DATE", "SORT_NAME_RO", "DISPLAY",
    "NAME_RANK", "FULL_NAME_RO", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT", "NOTE")

 def getUSA(sess : SparkSession): DataFrame = {
   val usa = extractFile(sess, new File("./data/places/nga/POP_PLACES_20170601.txt"), true, false,'|')
   val pipe = new Lineage("nga-usa")
   pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> usaSchema, "size" -> usaSchema.length, "delimiter" -> '|')
   pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "US")
   val m = pipe.fit(usa)

   val usaResult = m.transform(usa).withColumnRenamed("FEATURE_NAME", "name").select("name", "country")
   usaResult
 }
}
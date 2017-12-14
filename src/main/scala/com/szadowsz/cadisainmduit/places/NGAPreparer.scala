package com.szadowsz.cadisainmduit.places

import java.io.{File, StringReader}

import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.{CsvReader, FReader}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.{Lineage, LocalDataframeIO}
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.ulster.spark.transformers.string.{RegexGroupExtractor, StringFiller, StringMapper, StringTrimmer}
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColRenamerTransformer, CsvTransformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 27/04/2016.
  */
object NGAPreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

  val ngaSchema = Array("RC", "UFI", "UNI", "LAT", "LONG", "DMS_LAT", "DMS_LONG", "MGRS", "JOG", "FC", "DSG", "PC", "CC1", "ADM1", "POP", "ELEV", "CC2", "NT", "LC", "SHORT_FORM",
    "GENERIC", "SORT_NAME_RO", "FULL_NAME_RO", "FULL_NAME_ND_RO", "SORT_NAME_RG", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NOTE", "MODIFY_DATE", "DISPLAY", "NAME_RANK",
    "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT")

  val dropList = List("UFI", "UNI", "LAT", "LONG", "MGRS", "JOG", "DSG", "PC", "ADM1", "POP", "ELEV", "CC2", "MODIFY_DATE", "SORT_NAME_RO", "DISPLAY",
    "NAME_RANK", "FULL_NAME_RO", "FULL_NAME_RG", "FULL_NAME_ND_RG", "NAME_LINK", "TRANSL_CD", "NM_MODIFY_DATE", "F_EFCTV_DT", "F_TERM_DT", "NOTE")

  def getOtherCountries(sess : SparkSession):DataFrame = {
    val placeFiles = FileFinder.search("./data/places/nga", Some(new ExtensionFilter(".txt", false)))
      .filter(f => f.getName.endsWith("_populatedplaces_p.txt")
      )

    //   val placesDfs = placeFiles.map(f => extractFile(sess, f, true, true)).slice(0,placeFiles.length/4)
    val placesDfs = placeFiles.map(f => extractFile(sess, f, true, false,'\t'))

    val lcFunct = udf[Boolean, String, String]((s1: String, s2: String) => {
      (s1 == null || s1.trim.length == 0 || s1 == "eng") &&
        "[0-9]".r.findFirstMatchIn(s2).isEmpty && "^['a-z]".r.findFirstMatchIn(s2).isEmpty
    })

    val cutdownDfs = placesDfs.map { placesDf =>

      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(ngaSchema).setSize(ngaSchema.length).setDelimiter('\t')
      val csvDF = t.transform(placesDf)
      val shortDf = csvDF.drop(dropList: _*)
      val filterDf = shortDf.filter(lcFunct(col("LC"), col("FULL_NAME_ND_RO")))
      filterDf.drop("LC")
    }
    cutdownDfs.tail.foldLeft(cutdownDfs.head) { case (df1, df2) => df1.union(df2).toDF() }
      .withColumnRenamed("FULL_NAME_ND_RO", "name").withColumnRenamed("CC1", "country").select("name", "country")
  }
}
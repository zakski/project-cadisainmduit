package com.szadowsz.cadisainmduit.places

import java.io.File

import com.szadowsz.cadisainmduit.places.PlacePreparer.writeDF
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import com.szadowsz.spark.ml.feature.{CsvColumnExtractor, StringFiller}
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created on 27/04/2016.
  */
object OGPlacePreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

  val openGovSchema = Array("ID","NAMES_URI","NAME1","NAME1_LANG","NAME2","NAME2_LANG","TYPE","LOCAL_TYPE","GEOMETRY_X","GEOMETRY_Y","MOST_DETAIL_VIEW_RES",
    "LEAST_DETAIL_VIEW_RES","MBR_XMIN","MBR_YMIN", "MBR_XMAX","MBR_YMAX","POSTCODE_DISTRICT","POSTCODE_DISTRICT_URI","POPULATED_PLACE","POPULATED_PLACE_URI",
    "POPULATED_PLACE_TYPE","DISTRICT_BOROUGH","DISTRICT_BOROUGH_URI", "DISTRICT_BOROUGH_TYPE","COUNTY_UNITARY","COUNTY_UNITARY_URI","COUNTY_UNITARY_TYPE",
    "REGION","REGION_URI","COUNTRY","COUNTRY_URI","RELATED_SPATIAL_OBJECT","SAME_AS_DBPEDIA","SAME_AS_GEONAMES")

  val placeUKSchema = Array("NAME1","NAME1_LANG","NAME2","NAME2_LANG","TYPE","country")

  val selectList = List("NAME1","NAME1_LANG","NAME2","NAME2_LANG","TYPE","country")

  def getUK(sess : SparkSession): DataFrame = {
    val df = extractFile(sess,new File("./archives/data/opengov/places/placesUK.csv"),false)
    val pipe = new Lineage("placeUk")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> placeUKSchema, "size" -> placeUKSchema.length)
    val m = pipe.fit(df)
    val namingUDF = udf[String,String,String,String,String]((n1 : String,l1 : String, n2 : String, l2 : String) => if(n2 == null) n1 else if (l1 == "eng") n1 else n2)
    m.transform(df).select(namingUDF(col("NAME1"),col("NAME1_LANG"),col("NAME2"),col("NAME2_LANG")).alias("name"),col("country"))
  }

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()
    DeleteUtil.delete(new File("./data/places/og"))
    ZipperUtil.unzip("./archives/data/opengov/places/opname_csv_gb.zip", "./data/places/og")
    val placeFiles = FileFinder.search("./data/places/og/DATA", Some(new ExtensionFilter(".csv", false)))

    val typeUDF = udf[Boolean,String]((s : String) => s == "populatedPlace")

    val pipe = new Lineage("open-gov")
    pipe.addStage(classOf[CsvColumnExtractor], "inputCol" -> "fields", "outputCols" -> openGovSchema, "size" -> openGovSchema.length)
    pipe.addStage(classOf[StringFiller], "outputCol" -> "country", "value" -> "uk")

    //   val placesDfs = placeFiles.map(f => extractFile(sess, f, true, true)).slice(0,placeFiles.length/4)
    val placesDfs = placeFiles.foreach{f =>
      val placesDf = extractFile(sess, f, false)
     val model = pipe.fit(placesDf)
      val csvDF = model.transform(placesDf)
      writeDF(
        csvDF.select(selectList.head,selectList.tail: _*).filter(typeUDF(col("TYPE"))),
        "./data/places/placesUK.csv",
        "UTF-8",
        (row: Seq[String]) => true,
        Ordering.by((s: Seq[String]) =>s.head),
        true
      )
     }
  //  placesDfs.tail.foldLeft(placesDfs.head) { case (df1, df2) => df1.union(df2).toDF() }
  }
}
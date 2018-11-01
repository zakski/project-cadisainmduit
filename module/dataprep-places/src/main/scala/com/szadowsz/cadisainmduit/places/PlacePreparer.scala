package com.szadowsz.cadisainmduit.places

import java.io.File

import com.szadowsz.cadisainmduit.places.pop.OGPopPreparer
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.spark.ml.feature.{CastTransformer, NullReplacer, RegexGroupExtractor, RegexValidator}
import com.szadowsz.spark.ml.{Lineage, LocalDataframeIO}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created on 27/04/2016.
  */
object PlacePreparer extends LocalDataframeIO {
  private val _logger = LoggerFactory.getLogger(getClass)

 def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

   val africa = extractFile(sess,new File("./data/results/af_placenames.csv"),true,true).withColumnRenamed("total","africa")
   val americas = extractFile(sess,new File("./data/results/am_placenames.csv"),true,true).withColumnRenamed("total","americas")
   val asia = extractFile(sess,new File("./data/results/as_placenames.csv"),true,true).withColumnRenamed("total","asia")
   val europe = extractFile(sess,new File("./data/results/eur_placenames.csv"),true,true).withColumnRenamed("total","europe")
   val oceania = extractFile(sess,new File("./data/results/oa_placenames.csv"),true,true).withColumnRenamed("total","oceania")

   val joined = africa.join(americas,Seq("name"),"outer").join(asia,Seq("name"),"outer").join(europe,Seq("name"),"outer").join(oceania,Seq("name"),"outer")

   val typer = new Lineage()
   joined.columns.tail.foreach { n =>
     typer.addStage(new CastTransformer().setOutputDataType(LongType).setInputCol(n))
   }
   typer.addStage(new NullReplacer().setInputCols(joined.columns.tail).setReplacement(0L))


   val result = typer.fitAndTransform(joined)._2.select(
     col("name"),
     (col("africa") + col("americas") + col("asia") + col("europe") + col("oceania")).alias("total"),
     col("africa"),
     col("americas"),
     col("asia"),
     col("europe"),
     col("oceania")
   ).filter(col("total") =!= col("asia"))

   writeDF(result,"./data/results/placenames.csv","UTF-8",x => true,Ordering.by(seq => (-seq(1).toInt,seq.head)))
 }
}

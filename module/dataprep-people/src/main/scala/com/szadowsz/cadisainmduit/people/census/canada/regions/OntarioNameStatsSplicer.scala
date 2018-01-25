package com.szadowsz.cadisainmduit.people.census.canada.regions

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.spark.ml.Lineage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created on 04/05/2017.
  */
object OntarioNameStatsSplicer extends CensusHandler {

 val root = "./data/data/census/canada/ontario_firstnames"

   protected def getInitialCols : Array[String] = {
    Array("year", "name", "OT_count")
  }
  def aggStateData(sess: SparkSession, df : DataFrame): DataFrame = {
    df.groupBy("name","gender").pivot("year").sum("count")
//
//
//    val grouped = df.rdd.groupBy(r => (r.getString(nameI), r.getString(genderI)))
//        val expanded = grouped.map { case (k, rows) =>
//          val inter = rows.toArray.sortBy(r => r.getString(yearI).toInt).map(r => r.getString(yearI) -> r.getString(countI)).toMap
//          val counts = range.map(i => inter.getOrElse(i.toString, null))
//          Row.fromSeq(k._1 +: k._2 +: counts)
//        }
//        val schema = StructType(StructField("name", StringType) +: StructField("gender", StringType) +: range.map(y => StructField(s"OT_count_$y",StringType)
//        ).toArray)
//        sess.createDataFrame(expanded, schema)
  }

  def loadData(save: Boolean): DataFrame = {
   val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val mfiles = FileFinder.search(s"$root/boys", Some(new ExtensionFilter(".csv", false)))
    val ffiles = FileFinder.search(s"$root/girls", Some(new ExtensionFilter(".csv", false)))

    val mdf = extractFile(sess,mfiles.head,true,false)
    val mpipe: Lineage = buildStdPipeline("OT-M-caps", getInitialCols)
    val mmodel = mpipe.fit(mdf)
    val boys = mmodel.transform(mdf).dropDuplicates(Array("name","year")).withColumn("gender",lit("M"))

    val fdf = extractFile(sess,ffiles.head,true,false)
    val fpipe: Lineage = buildStdPipeline("OT-M-caps", getInitialCols)
    val fmodel = fpipe.fit(fdf)
    val girls = fmodel.transform(fdf).dropDuplicates(Array("name","year")).withColumn("gender",lit("M"))

    val bcDF = aggStateData(sess,boys.union(girls).select(col("name"),col("gender"),col("year"),col("OT_Count").cast(IntegerType).name("count")))
//
//    val appFields = bcDF.schema.fieldNames.filterNot(f => f == "name" || f == "gender")
//
//    val pipe: Lineage = buildFractionPipeline(s"OT-frac","OT", appFields,appFields)
//    val m = pipe.fit(bcDF)
    val children = bcDF//m.transform(bcDF)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
      writeDF(children, s"./data/tmp/OT/baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }


    children
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

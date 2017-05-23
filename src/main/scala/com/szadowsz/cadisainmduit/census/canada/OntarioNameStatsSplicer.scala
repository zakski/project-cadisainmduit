package com.szadowsz.cadisainmduit.census.canada

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created on 04/05/2017.
  */
object OntarioNameStatsSplicer extends CensusHandler {

  val archive = "./archives/data/census/canada/ontario_firstnames.zip"
  val root = "./data/census/canada/ot"

   protected def getInitialCols : Array[String] = {
    Array("year", "name", "OT_count")
  }
  def aggStateData(sess: SparkSession, df : DataFrame): DataFrame = {
    val range = 1917 to 2013
    val nameI = df.schema.fieldNames.indexOf("name")
        val genderI = df.schema.fieldNames.indexOf("gender")
        val yearI = df.schema.fieldNames.indexOf("year")
        val countI = df.schema.fieldNames.indexOf("OT_count")

        val grouped = df.rdd.groupBy(r => (r.getString(nameI), r.getString(genderI)))
        val expanded = grouped.map { case (k, rows) =>
          val inter = rows.toArray.sortBy(r => r.getString(yearI).toInt).map(r => r.getString(yearI) -> r.getString(countI)).toMap
          val counts = range.map(i => inter.getOrElse(i.toString, null))
          Row.fromSeq(k._1 +: k._2 +: counts)
        }
        val schema = StructType(StructField("name", StringType) +: StructField("gender", StringType) +: range.map(y => StructField(s"OT_count_$y",StringType)
        ).toArray)
        sess.createDataFrame(expanded, schema)
  }

  def loadData(save: Boolean): DataFrame = {
    DeleteUtil.delete(new File(s"$root/boys"))
    DeleteUtil.delete(new File(s"$root/girls"))
    ZipperUtil.unzip(archive, root + "/")

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val mfiles = FileFinder.search(s"$root/boys", Some(new ExtensionFilter(".csv", false)))
    val ffiles = FileFinder.search(s"$root/girls", Some(new ExtensionFilter(".csv", false)))

    val mdf = extractFile(sess,mfiles.head,true,false)
    val mpipe: Lineage = buildStdPipeline("OT-M-caps", getInitialCols, Option('M'))
    val mmodel = mpipe.fit(mdf)
    val boys = mmodel.transform(mdf).dropDuplicates(Array("name","gender"))

    val fdf = extractFile(sess,ffiles.head,true,false)
    val fpipe: Lineage = buildStdPipeline("OT-M-caps", getInitialCols, Option('F'))
    val fmodel = fpipe.fit(fdf)
    val girls = fmodel.transform(fdf).dropDuplicates(Array("name","gender"))

    val bcDF = aggStateData(sess,boys.union(girls))

    val appFields = bcDF.schema.fieldNames.filterNot(f => f == "name" || f == "gender")

    val pipe: Lineage = buildFractionPipeline(s"OT-frac","OT", appFields,appFields)
    val m = pipe.fit(bcDF)
    val children = m.transform(bcDF)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.last, seq.head))
      writeDF(children, s"$root/baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }

    DeleteUtil.delete(new File(s"$root/boys"))
    DeleteUtil.delete(new File(s"$root/girls"))
    children
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

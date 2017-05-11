package com.szadowsz.cadisainmduit.census.canada

import java.io.File

import com.szadowsz.cadisainmduit.census.CensusHandler
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.Lineage
import org.apache.spark.sql.{DataFrame, SparkSession}

object BCNamesStatsSplicer extends CensusHandler {
  val archive = "./archives/data/census/canada/bc_firstnames.zip"
  val root = "./data/census/canada/bc"

  protected def getInitialCols : Array[String] = {
    val years = (1916 to 2015).map(_.toString).toArray
    "name" +: years :+ "total"
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
    val mpipe: Lineage = buildStdPipeline("BC-M-caps", getInitialCols, Option('M'))
    val mmodel = mpipe.fit(mdf)
    val boys = mmodel.transform(mdf).dropDuplicates(Array("name","gender"))

    val fdf = extractFile(sess,ffiles.head,true,false)
    val fpipe: Lineage = buildStdPipeline("BC-M-caps", getInitialCols, Option('F'))
    val fmodel = fpipe.fit(fdf)
    val girls = fmodel.transform(fdf).dropDuplicates(Array("name","gender"))

    val bcDF = boys.union(girls).drop("total")

    val appFields = bcDF.schema.fieldNames.filterNot(f => f == "name" || f == "gender")

    val pipe: Lineage = buildFractionPipeline(s"BC-frac","BC", appFields,appFields)
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


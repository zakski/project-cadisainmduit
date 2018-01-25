package com.szadowsz.cadisainmduit.people.census.canada.regions

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.spark.ml.Lineage
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BCNamesStatsSplicer extends CensusHandler {
  val root = "./data/data/census/canada/bc_firstnames"

  /**
    * Method to get the initial columns
    *
    * @return array of columns
    */
  protected def getInitialCols: Array[String] = {
    val years = (1916 to 2015).map(_.toString).toArray
    "name" +: years :+ "total"
  }

  /**
    * Loads and Prepares the Alberta Dataset
    *
    * @param save whether to dump the data to a tmp folder
    * @return the prepared dataset
    */
  def loadData(save: Boolean): DataFrame = {

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()

    val mfiles = FileFinder.search(s"$root/boys", Some(new ExtensionFilter(".csv", false)))
    val ffiles = FileFinder.search(s"$root/girls", Some(new ExtensionFilter(".csv", false)))

    val mdf = extractFile(sess, mfiles.head, true, false)
    val mpipe: Lineage = buildStdPipeline("BC-M-caps", getInitialCols)
    val mmodel = mpipe.fit(mdf)
    val boys = mmodel.transform(mdf).dropDuplicates(Array("name")).withColumn("gender",lit("M"))

    val fdf = extractFile(sess, ffiles.head, true, false)
    val fpipe: Lineage = buildStdPipeline("BC-M-caps", getInitialCols)
    val fmodel = fpipe.fit(fdf)
    val girls = fmodel.transform(fdf).dropDuplicates(Array("name")).withColumn("gender",lit("F"))

    val bcDF = boys.union(girls).select("name", ("gender" +: (1916 to 2015).map(_.toString).toArray):_*) //.drop("total")

    //
    //    val pipe: Lineage = buildFractionPipeline(s"BC-frac","BC", appFields,appFields)
    //    val appFields = bcDF.schema.fieldNames.filterNot(f => f == "name" || f == "gender")
    //    val m = pipe.fit(bcDF)
    val children = bcDF // m.transform(bcDF)

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => (seq.head))
      writeDF(children, s"./data/tmp/BC/baby_names.csv", "UTF-8", (seq: Seq[String]) => seq.head.length > 0, ord)
    }

    children
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}


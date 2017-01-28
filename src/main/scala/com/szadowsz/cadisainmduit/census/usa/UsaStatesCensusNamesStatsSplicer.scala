package com.szadowsz.cadisainmduit.census.usa

import java.io.File

import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.math.{CounterTransformer, DivisionTransformer}
import com.szadowsz.ulster.spark.transformers.CsvTransformer
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import com.szadowsz.common.io.delete.DeleteUtil
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.common.io.zip.ZipperUtil
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaStatesCensusNamesStatsSplicer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  val ver = List("VT")
  val bSky = List("ND", "SD", "NE", "KS")
  val pac = List("CA", "OR", "WA", "NV", "AK", "HI")
  val front = List("MT", "WY", "ID", "UT", "CO", "AZ")
  val mWest = List("MI", "OH", "IN", "IL", "WI", "MN", "IA","MO")
  val tx = List("TX", "NM", "OK", "LA", "AR", "MS", "AL", "FL", "TN", "KY")
  val thrtn = List("DE", "SC", "PA", "NJ", "GA", "CT", "MA", "ME", "MD", "DC", "NH", "VA", "NY", "NC", "RI", "WV")

  protected def extractFile(sess: SparkSession, f: File): (String, DataFrame) = {
    val state = f.getName.substring(0, 2)
    val r = new FReader(f.getAbsolutePath)
    val stringRdd = sess.sparkContext.parallelize(r.lines().toArray) // assume the first line is the header
    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache() // cache the constructed dataframe
    (state, df)
  }

  protected def buildNamePipeline(country: String, state: String): Lineage = {
    val pipe = new Lineage(state)
    val cols = buildCols(country, state)
    pipe.addStage(classOf[CsvTransformer], Map("inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length))
    pipe.addStage(classOf[CapitalisationTransformer], Map("inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all"))
    pipe.addStage(classOf[RegexValidationTransformer], Map("inputCol" -> "nameCap", "pattern" -> "^\\p{L}+$"))
    pipe
  }

  protected def buildCols(country: String, state: String): Array[String] = {
    Array("state", "gender", "year", "name", s"${state}_count")
  }

  protected def aggStateData(save: Boolean, sess: SparkSession, path: String, country: String): Map[String, DataFrame] = {
    val files = FileFinder.search(path, Some(new ExtensionFilter(".TXT", false)))
    val range = 1910 to 2015
    files.map { f =>
      val (state, df) = extractFile(sess, f)

      val pipe: Lineage = buildNamePipeline(country, state)

      val model = pipe.fit(df)
      val tmp = model.transform(df).drop("name").withColumnRenamed("nameCap", "name")

      val nameI = tmp.schema.fieldNames.indexOf("name")
      val genderI = tmp.schema.fieldNames.indexOf("gender")
      val yearI = tmp.schema.fieldNames.indexOf("year")
      val countI = tmp.schema.fieldNames.indexOf(s"${state}_count")

      val grouped = tmp.rdd.groupBy(r => (r.getString(nameI), r.getString(genderI)))
      val expanded = grouped.map { case (k, rows) =>
        val inter = rows.toArray.sortBy(r => r.getString(yearI).toInt).map(r => r.getString(yearI) -> r.getString(countI)).toMap
        val counts = range.map(i => inter.getOrElse(i.toString, null))
        Row.fromSeq(k._1 +: k._2 +: counts)
      }
      val schema = StructType(StructField("name", StringType) +: StructField("gender", StringType) +: range.map(y => StructField(s"${state}_count_$y",
        StringType)).toArray)
      val stateDf = sess.createDataFrame(expanded, schema)

      if (save) {
        val writer = new CsvWriter(s"./data/census/us/states/babynames_${state}.csv", "UTF-8", false)
        writer.write(stateDf.schema.fieldNames: _*)
        val res = stateDf.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
        writer.writeAll(res.sortBy(seq => (seq.head)))
        writer.close()
      }
      (state, stateDf)
    }.toMap
  }

  protected def aggRegData(save: Boolean, sess: SparkSession, sMap : Map[String, DataFrame], reg: String, states : List[String]): DataFrame = {
    logger.info(s"Beginning Merge of Region Data : $reg")
    val stateDfs = states.map(sMap(_))
    val regDf = stateDfs.tail.foldLeft(stateDfs.head) { case (comp, curr) =>
      comp.join(curr, Seq("name", "gender"), "outer")
    }
    regDf.cache()
    logger.info(s"Finished Merge of Region Data : $reg")
    val count = regDf.schema.length - 2
    val counts = regDf.schema.fieldNames.filter(f => f.contains("count"))

    val pipe = new Lineage("comb")
    pipe.addStage(classOf[CounterTransformer], Map("neverNull" -> 2, "outputCol" -> "appearCount"))
    pipe.addStage(classOf[DivisionTransformer], Map("outputCol" -> s"${reg}_appearFrac", "inputCol" -> "appearCount", "total" -> count.toDouble, "decPlaces" ->
      3))
    val m = pipe.fit(regDf)
    val children = m.transform(regDf).select("name", ("gender" +: s"${reg}_appearFrac" +: counts): _*)
    val casted = counts.foldLeft(children) { case (df, f) =>
      df.withColumn("tmp", df(f).cast(IntegerType)).drop(f).withColumnRenamed("tmp", f).drop("tmp")
    }.na.fill(0.0)
    logger.info(s"Finished Counting nulls for Region Data : $reg")

    val pipe2 = new Lineage("comb2")
    pipe2.addStage(classOf[VectorAssembler], Map("inputCols" -> counts, "outputCol" -> "vec"))
    pipe2.addStage(classOf[AverageTransformer], Map("inputCol" -> "vec", "excludeZeros" -> true, "outputCol" -> s"${reg}_avgVal", "decPlaces" -> 3))
    val m2 = pipe2.fit(casted)
    val all = m2.transform(casted).drop(("vec" +: counts):_*)


    if (save) {
      logger.info(s"Collecting Region Data : $reg")
      val writer = new CsvWriter(s"./data/census/us/babynames_${reg}.csv", "UTF-8", false)
      writer.write(all.schema.fieldNames: _*)
      val res = all.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      logger.info(s"Writing Region : $reg")
      writer.writeAll(res.sortBy(seq => (seq.head)))
      writer.close()
    }
    all
  }

  protected def selectYearCols(country: String, year: String, tmp: DataFrame): DataFrame = {
    tmp.select("name", "gender", s"${country}_count_$year")
  }

  def loadData(save: Boolean): DataFrame = {
    val statesCount = bSky.length + tx.length + front.length + ver.length + pac.length + mWest.length + thrtn.length
    require(statesCount == 51, s"$statesCount != 51")
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
    ZipperUtil.unzip(s"./archives/data/census/us/namesByState.zip", "./data/census/us/namesByState")

    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[8]")
      .getOrCreate()
    val stateMap = aggStateData(false, sess, "./data/census/us/namesByState", "us")

    val vr = aggRegData(save,sess,stateMap,"VR", ver)
    val bs = aggRegData(save,sess,stateMap,"BS", bSky)
    val pc = aggRegData(save,sess,stateMap,"PC", pac)
    val ww = aggRegData(save,sess,stateMap,"WW", front)
    val mw = aggRegData(save,sess,stateMap,"MW", mWest)
    val te = aggRegData(save,sess,stateMap,"TE", tx)
    val tt = aggRegData(save,sess,stateMap,"13", thrtn)
  //
    //    if (yearData.count() < 1) {
    //      DeleteUtil.delete(new File("./data/census/us/names/"))
    //      throw new RuntimeException(yearData.schema.fieldNames.mkString("|"))
    //    }
    //
    //    if (save) {
    //      val writer = new CsvWriter("./data/census/us/babyames.csv", "UTF-8", false)
    //      writer.write(yearData.schema.fieldNames: _*)
    //      val res = yearData.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
    //      writer.writeAll(res.sortBy(seq => (seq.head)))
    //      writer.close()
    //    }
    DeleteUtil.delete(new File("./data/census/us/namesByState/"))
    //  yearData
    null
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }

  //  ZipperUtil.unzip("./archives/census/us/names.zip", "./data/census/us/names/")
  //      ZipperUtil.unzip("./archives/census/us/namesByState.zip", "./data/census/us/namesByState/")
}

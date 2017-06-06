package com.szadowsz.cadisainmduit.people.census.usa.states

import java.io.File

import com.szadowsz.cadisainmduit.people.census.CensusHandler
import com.szadowsz.common.io.explore.{ExtensionFilter, FileFinder}
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.math.NullTransformer
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import com.szadowsz.ulster.spark.transformers.{ColFilterTransformer, CsvTransformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, _}
import org.slf4j.LoggerFactory

/**
  * Created on 19/10/2016.
  */
object UsaRegionUtil extends CensusHandler {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def buildCols(state: String): Array[String] = {
    Array("state", "gender", "year", "name", s"${state}_count")
  }

  def x() = {

  }

  def aggStateData(save: Boolean, sess: SparkSession, inpath: String, outpath: String, states: List[String]): Seq[DataFrame] = {
    val files = FileFinder.search(inpath, Some(new ExtensionFilter(".TXT", false)))
    val range = 1910 to 2015
    val reg = files.map(f => (f.getName.substring(0, 2), f)).filter(s => states.isEmpty || states.contains(s._1)).map { case (state, f) =>
      logger.info(s"Parsing : $state")
      if (!new File(s"$outpath/babynames_${state}.csv").exists()) {
        val df = extractFile(sess, f,false)

        val pipe: Lineage = buildStdPipeline(s"$state-shuffle", buildCols(state), None)

        val model = pipe.fit(df)
        val tmp = model.transform(df)

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
          val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
          writeDF(stateDf, s"$outpath/babynames_${state}.csv", "UTF-8", f => f.head.length > 0, ord)
        }
        stateDf
      } else {
        val df = extractFile(sess, new File(s"$outpath/babynames_$state.csv"),true)
        val cols = Array("name", "gender") ++ range.map(i => s"${state}_count_$i")
        val pipe = new Lineage(s"$state-load")
        pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
        val model = pipe.fit(df)
        model.transform(df)
      }
    }
    reg
  }

  def aggRegData(save: Boolean, sess: SparkSession, inpath: String, outpath: String, reg: String, states: List[String]): DataFrame = {
    logger.info(s"Beginning Merge of Region Data : $reg")
    val stateDFs = states.zip(aggStateData(save, sess, inpath, outpath, states))
    val regDf =  join(stateDFs.map{ case (s,df) =>
      val popFields = df.schema.fieldNames.filter(f => f.contains("count"))
      val pipe = buildFractionPipeline(s"$s-frac", s, popFields, popFields)
      val m = pipe.fit(df)
      m.transform(df)
    })

    regDf.cache()
    logger.info(s"Finished Merge of Region Data : $reg")

    val appFields = regDf.schema.fieldNames.filter(f => f.contains("appFrac"))
    val avgFields = regDf.schema.fieldNames.filter(f => f.contains("avgVal"))

    val pipe = new Lineage(s"$reg-frac")
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], "inputCols" -> appFields, "outputCol" -> "fracs")
    pipe.addStage(classOf[VectorAssembler], "inputCols" -> avgFields, "outputCol" -> "avgs")
    pipe.addStage(classOf[AverageTransformer], "inputCol" -> "fracs", "excludeZeros" -> false, "outputCol" -> s"${reg}_appfrac", "decPlaces" -> 3)
    pipe.addStage(classOf[AverageTransformer], "inputCol" -> "avgs", "excludeZeros" -> true, "outputCol" -> s"${reg}_avgVal", "decPlaces" -> 2)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> (Array("fracs", "avgs") ++ appFields ++ avgFields), "isInclusive" -> false)

    val m = pipe.fit(regDf)
    val all = m.transform(regDf)
    logger.info(s"Finished Counting nulls for Region Data : $reg")

    if (save) {
      val ord: Ordering[Seq[String]] = Ordering.by(seq => seq.head)
      writeDF(all, s"./data/census/us/babynames_${reg}.csv", "UTF-8", f => f.head.length > 0, ord)
    }
    all
  }


}

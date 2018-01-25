package com.szadowsz.cadisainmduit.people.census.uk

import com.szadowsz.cadisainmduit.people.census.uk.engwales.{EngWalNamesFreqCalculator, EngWalNamesStatsSplicer}
import com.szadowsz.cadisainmduit.people.census.uk.norire.{NorireNamesFreqCalculator, NorireNamesStatsSplicer}
import com.szadowsz.cadisainmduit.people.census.uk.scotland.{ScotGirlNamesStatsSplicer, ScotNamesFreqCalculator}
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.spark.ml.Lineage
import com.szadowsz.spark.ml.feature.{AverageTransformer, ColFilterTransformer, NullReplacer, RoundingTransformer}
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, NumericType}
import org.apache.spark.sql.functions._


/**
  * Created on 16/01/2017.
  */
object UkCensusNamesFreqCalculator {

  def loadData(save: Boolean): DataFrame = {
    val ni = NorireNamesFreqCalculator.loadData(false)
    val scot = ScotNamesFreqCalculator.loadData(false)
    val engwal = EngWalNamesFreqCalculator.loadData(false)


    val uk = ni.join(scot, Seq("name", "gender"), "outer").join(engwal, Seq("name", "gender"), "outer").na.fill("unused").cache()

    val fracs = uk.columns.filter(_.contains("AppRank"))

    val pipe = new Lineage("uk")
    fracs.foreach(r => pipe.addStage(new StringIndexerModel(s"$r-Indexer",Array("unused","rare","uncommon","common","basic")).setInputCol(r).setOutputCol(r + "Ind")))
    pipe.addStage(classOf[AverageTransformer],"inputCols" -> fracs.map(_ + "Ind").toArray, "outputCol" -> "UK_RawAppRank")
    pipe.addStage(classOf[RoundingTransformer],"inputCol" -> "UK_RawAppRank", "outputCol" -> "UK_RoundedAppRank")
    pipe.addStage(classOf[IndexToString],"inputCol" -> "UK_RoundedAppRank", "outputCol" -> "UK_AppRank", "labels" -> Array("unused","rare","uncommon","common","basic"))
    val (model,ukDf) = pipe.fitAndTransform(uk)
    val df = ukDf.select("name","gender", "EW_AppRank","SC_AppRank","NI_AppRank","UK_AppRank")

    if (save) {
      val writer = new CsvWriter("./data/results/uk_baby_names.csv", "UTF-8", false)
      writer.write(df.schema.fieldNames: _*)
      val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      writer.writeAll(res.sortBy(seq => (seq.head)))
      //writer.write(tots: _*)
      writer.close()
    }

    df
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

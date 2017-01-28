package com.szadowsz.cadisainmduit.census.uk

import com.szadowsz.cadisainmduit.census.uk.engwales.EngWalNamesStatsSplicer
import com.szadowsz.cadisainmduit.census.uk.norire.NorireNamesStatsSplicer
import com.szadowsz.cadisainmduit.census.uk.scotland.ScotNamesStatsSplicer
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.transformers.ColFilterTransformer
import com.szadowsz.ulster.spark.transformers.math.NullTransformer
import com.szadowsz.ulster.spark.transformers.math.vec.{AverageTransformer, VecSumTransformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, NumericType}
import org.apache.spark.sql.functions._


/**
  * Created on 16/01/2017.
  */
object UkCensusNamesStatsSplicer {

  def loadData(save: Boolean): DataFrame = {
    val ni = NorireNamesStatsSplicer.loadData(save)
    val scot = ScotNamesStatsSplicer.loadData(save)
    val engwal = EngWalNamesStatsSplicer.loadData(save)


    val uk = ni.join(scot, Seq("name", "gender"), "outer").join(engwal, Seq("name", "gender"), "outer").na.fill(0.0).cache()

    val numerics = uk.schema.filter(f => f.dataType.isInstanceOf[NumericType]).map(_.name)
    val fracs = numerics.filter(_.contains("appFrac"))
    val avg = numerics.filter(_.contains("avgVal"))

    val pipe = new Lineage("uk")
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], Map("inputCols" -> fracs.toArray, "outputCol" -> "uk_appVec"))
    pipe.addStage(classOf[AverageTransformer], Map("inputCol" -> "uk_appVec", "outputCol" -> "UK_appFrac", "decPlaces" -> 3))
    pipe.addStage(classOf[VectorAssembler], Map("inputCols" -> avg.toArray, "outputCol" -> "uk_avgVec"))
    pipe.addStage(classOf[AverageTransformer], Map("inputCol" -> "uk_avgVec", "outputCol" -> "UK_avgVal", "decPlaces" -> 3))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("uk_appVec","uk_avgVec"), "isInclusive" -> false)
    val model = pipe.fit(uk)
    var df = model.transform(uk)

    if (save) {
      val writer = new CsvWriter("./data/census/uk_baby_names.csv", "UTF-8", false)
      writer.write(df.schema.fieldNames: _*)
      val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(_.head.length > 0)
      writer.writeAll(res.sortBy(seq => (seq.head)))
      //writer.write(tots: _*)
      writer.close()
    }

    uk
  }

  def main(args: Array[String]): Unit = {
    loadData(true)
  }
}

package com.szadowsz.cadisainmduit.census.canada

import com.szadowsz.cadisainmduit.census.uk.engwales.EngWalNamesStatsSplicer
import com.szadowsz.cadisainmduit.census.uk.norire.NorireNamesStatsSplicer
import com.szadowsz.cadisainmduit.census.uk.scotland.ScotNamesStatsSplicer
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.ColFilterTransformer
import com.szadowsz.ulster.spark.transformers.math.NullTransformer
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.NumericType


/**
  * Created on 16/01/2017.
  */
object CanadaCensusNamesStatsSplicer {

  def loadData(save: Boolean): DataFrame = {
    val alb = AlbertaNamesStatsSplicer.loadData(save)
    val bc = BCNamesStatsSplicer.loadData(save)
    val ot = OntarioNameStatsSplicer.loadData(save)

    val cn = alb.join(bc, Seq("name", "gender"), "outer").join(ot, Seq("name", "gender"), "outer").na.fill(0.0).cache()

    val numerics = cn.schema.filter(f => f.dataType.isInstanceOf[NumericType]).map(_.name)
    val fracs = numerics.filter(_.contains("appFrac"))
    val avg = numerics.filter(_.contains("avgVal"))

    val pipe = new Lineage("cn")
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], Map("inputCols" -> fracs.toArray, "outputCol" -> "cn_appVec"))
    pipe.addStage(classOf[AverageTransformer], Map("inputCol" -> "cn_appVec", "outputCol" -> "CN_appFrac", "decPlaces" -> 3))
    pipe.addStage(classOf[VectorAssembler], Map("inputCols" -> avg.toArray, "outputCol" -> "cn_avgVec"))
    pipe.addStage(classOf[AverageTransformer], Map("inputCol" -> "cn_avgVec", "outputCol" -> "CN_avgVal", "decPlaces" -> 3))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("cn_appVec", "cn_avgVec"), "isInclusive" -> false)
    val model = pipe.fit(cn)
    var df = model.transform(cn)

    if (save) {
      val writer = new CsvWriter("./data/census/cn_baby_names.csv", "UTF-8", false)
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

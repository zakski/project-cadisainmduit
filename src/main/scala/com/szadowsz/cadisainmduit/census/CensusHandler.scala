package com.szadowsz.cadisainmduit.census

import java.io.{File, StringReader}

import com.szadowsz.common.io.read.FReader
import com.szadowsz.common.io.write.CsvWriter
import com.szadowsz.ulster.spark.Lineage
import com.szadowsz.ulster.spark.transformers.math.vec.AverageTransformer
import com.szadowsz.ulster.spark.transformers.math.{CounterTransformer, DivisionTransformer, NullTransformer}
import com.szadowsz.ulster.spark.transformers.{CastTransformer, ColFilterTransformer, ColRenamerTransformer, CsvTransformer}
import com.szadowsz.ulster.spark.transformers.string.StringFiller
import com.szadowsz.ulster.spark.transformers.string.spelling.{CapitalisationTransformer, RegexValidationTransformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.supercsv.io.CsvListReader
import org.supercsv.prefs.CsvPreference

import scala.collection.JavaConverters._

/**
  * Created on 25/01/2017.
  */
trait CensusHandler {

  protected def convertToRDD(sess: SparkSession, dropFirst: Boolean, lines: Array[String]): (Array[String], RDD[String]) = {
    if (dropFirst) { // assume the first line is the header
      val schema = new CsvListReader(new StringReader(lines.head.asInstanceOf[String]), CsvPreference.STANDARD_PREFERENCE).read().asScala.toArray
      (schema, sess.sparkContext.parallelize(lines.drop(1)))
    } else {
      (Array(), sess.sparkContext.parallelize(lines))
    }
  }

  protected def extractFile(sess: SparkSession, f: File, dropFirst: Boolean, readSchema: Boolean = false): DataFrame = {
    val r = new FReader(f.getAbsolutePath)
    val lines = r.lines().toArray.map(_.toString)

    val (fields, stringRdd) = convertToRDD(sess, dropFirst, lines)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    var df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))

    if (readSchema && dropFirst) {
      val t = new CsvTransformer("X").setInputCol("fields").setOutputCols(fields).setSize(fields.length)
      df = t.transform(df)
    }
    df.cache() // cache the constructed dataframe
    df
  }

  /**
    * General Purpose Dataframe to csv file writer.
    *
    * @param df
    * @param path
    * @param charset
    * @param filter
    * @param sortBy
    */
  protected def writeDF(df: DataFrame, path: String, charset: String, filter: (Seq[String]) => Boolean, sortBy: Ordering[Seq[String]]): Unit = {
    val writer = new CsvWriter(path, charset, false)
    writer.write(df.schema.fieldNames: _*)
    val res = df.collect().map(r => r.toSeq.map(f => Option(f).map(_.toString).getOrElse(""))).filter(filter)
    writer.writeAll(res.sorted(sortBy))
    writer.close()
  }

  protected def buildStdPipeline(name: String, cols: Array[String], gender: Option[Char]): Lineage = {
    require(cols.contains("name") && (cols.contains("gender") || (gender.contains('M') || gender.contains('F'))), "Missing Default Column")
    val pipe = new Lineage(name)
    pipe.addStage(classOf[CsvTransformer], "inputCol" -> "fields", "outputCols" -> cols, "size" -> cols.length)
    pipe.addStage(classOf[CapitalisationTransformer], "inputCol" -> "name", "outputCol" -> "nameCap", "mode" -> "all")
    pipe.addStage(classOf[RegexValidationTransformer], "inputCol" -> "nameCap", "pattern" -> "^\\p{L}+$")
    gender.foreach(g => pipe.addStage(classOf[StringFiller], "outputCol" -> "gender", "value" -> g.toString))
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> Array("name"), "isInclusive" -> false)
    pipe.addStage(classOf[ColRenamerTransformer], "inputCols" -> Array("nameCap"), "outputCols" -> Array("name"))
    pipe
  }

  protected def buildFractionPipeline(name: String, country: String, appCols: Array[String], popCols: Array[String]): Lineage = {
    val pipe = new Lineage(name)
    appCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[CounterTransformer], "countValue" -> false, "value" -> null, "inputCols" -> appCols, "outputCol" -> "appearCount")

    val div = Map("outputCol" -> s"${country}_appFrac", "inputCol" -> "appearCount", "total" -> appCols.length.toDouble, "decPlaces" -> 3)
    pipe.addStage(classOf[DivisionTransformer], div)

    popCols.foreach(f => pipe.addStage(classOf[CastTransformer], "inputCol" -> f, "outputDataType" -> IntegerType))
    pipe.addStage(classOf[NullTransformer], "replacement" -> 0.0)
    pipe.addStage(classOf[VectorAssembler], "inputCols" -> popCols, "outputCol" -> "counts")
    pipe.addStage(classOf[AverageTransformer], "inputCol" -> "counts", "excludeZeros" -> true, "outputCol" -> s"${country}_avgVal", "decPlaces" -> 2)
    pipe.addStage(classOf[ColFilterTransformer], "inputCols" -> (Array("appearCount", "counts") ++ appCols), "isInclusive" -> false)
    pipe
  }


  protected def join(dfs: Seq[DataFrame]): DataFrame = {
    require(dfs.forall(df => df.schema.fieldNames.contains("name") && df.schema.fieldNames.contains("gender")), "Missing Default Column")
    dfs.tail.foldLeft(dfs.head) { case (comp, curr) => comp.join(curr, Seq("name", "gender"), "outer") }
  }
}
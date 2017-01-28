package com.szadowsz.ulster.spark.transformers.math

import com.szadowsz.ulster.spark.params.HasInputCols
import com.szadowsz.ulster.spark.transformers.math
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.LoggerFactory

/**
  * Created on 11/01/2017.
  */
class CounterTransformer(override val uid: String) extends Transformer with HasInputCols {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  protected val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  protected val value: Param[Double] = new Param[ Double](this, "value", "")

  protected val countValue : Param[Boolean] = new Param[Boolean](this,"countValue","")
  setDefault(countValue,true)

  def this() = this(Identifiable.randomUID("counter"))

   override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.filter(f => $(inputCols).contains(f.name)).forall(_.dataType.isInstanceOf[NumericType]))
    StructType(schema.fields :+ StructField($(outputCol), IntegerType))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val schema = transformSchema(dataset.schema)
    // Data transformation.
    val assembleFunc = if ($(countValue))
      udf { r: Row => r.toSeq.asInstanceOf[Seq[Double]].count(_ == $(value))}
    else
      udf { r: Row => r.toSeq.asInstanceOf[Seq[Double]].count(_ != $(value))}

    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    dataset.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol)))
  }


}

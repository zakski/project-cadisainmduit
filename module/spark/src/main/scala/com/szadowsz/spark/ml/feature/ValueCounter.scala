package com.szadowsz.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created on 11/01/2017.
  */
class ValueCounter(override val uid: String) extends Transformer with HasInputCols with HasOutputCol {
  protected val logger: Logger = LoggerFactory.getLogger("com.szadowsz.spark.ml")

  protected val value: Param[Double] = new Param[ Double](this, "value", "")

  protected val countValue: Param[Boolean] = new Param[Boolean](this, "countValue", "")
  setDefault(countValue, true)

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
      udf { r: Row => r.toSeq.asInstanceOf[Seq[Double]].count(_ == $(value)) }
    else
      udf { r: Row => r.toSeq.asInstanceOf[Seq[Double]].count(_ != $(value)) }

    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case DoubleType => dataset(c)
        case _: NumericType | BooleanType => dataset(c).cast(DoubleType).as(s"${c}_double_$uid")
      }
    }

    dataset.select(col("*"), assembleFunc(struct(args: _*)).as($(outputCol)))
  }


}

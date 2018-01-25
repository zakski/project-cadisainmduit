package com.szadowsz.spark.ml.feature

import java.text.DecimalFormat

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, NumericType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Created on 11/01/2017.
  */
class DivisionTransformer(override val uid: String) extends Transformer {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  def this() = this(Identifiable.randomUID("div"))

  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  protected val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  protected val decPlaces: Param[Int] = new Param[Int](this, "decPlaces", "number of decimal places")

  protected val total: Param[Double] = new Param[Double](this, "total", "total value")

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setTotal(tot: Double): this.type = set(total, tot)

  def setDecimalPlaces(places: Int): this.type = set(decPlaces, places)

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    } else if (!schema.fieldNames.contains($(inputCol))) {
      throw new IllegalArgumentException(s"Input column ${$(inputCol)} does not exist")
    } else if (!schema($(inputCol)).dataType.isInstanceOf[NumericType]) {
      throw new IllegalArgumentException(s"Input column ${$(inputCol)} must be Numeric")
    }

    val outputFields = schema.fields :+ StructField($(outputCol), DoubleType, nullable = false)
    StructType(outputFields)
  }

  protected def createFunc: Any => Double = (x: Any) => {
    val res = x match {
      case i: Int => i / $(total)
      case d: Double => d / $(total)
      case f: Float => f / $(total)
      case l: Long => l / $(total)
      case _ => x.toString.toDouble / $(total)
    }
    if (isDefined(decPlaces)) {
      val f = new DecimalFormat
      f.setGroupingUsed(false)
      f.setMaximumFractionDigits($(decPlaces))
      f.setMinimumFractionDigits($(decPlaces))
      f.format(res).toDouble
    } else {
      res
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}", uid)
    val schema = transformSchema(dataset.schema, logging = true)
    val transformUDF = udf(createFunc)
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

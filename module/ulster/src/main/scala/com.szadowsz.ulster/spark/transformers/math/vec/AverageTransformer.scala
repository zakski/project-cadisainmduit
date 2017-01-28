package com.szadowsz.ulster.spark.transformers.math.vec

import java.text.DecimalFormat

import com.szadowsz.ulster.spark.params.HasDecimalFormatting
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created on 17/01/2017.
  */
class AverageTransformer(override val uid: String) extends UnaryTransformer[Vector, Double, AverageTransformer] with HasDecimalFormatting {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  def this() = this(Identifiable.randomUID("avg"))

  protected val excludeZeros: Param[Boolean] = new Param[Boolean](this, "excludeZeros", "")
  setDefault(ParamPair(excludeZeros, false), ParamPair(roundingMode, "HALF_DOWN"))

  def setExcludeZeros(places: Boolean): this.type = set(excludeZeros, places)

  override protected def createTransformFunc: (Vector) => Double = {
    (v: Vector) => {
      val arr = if ($(excludeZeros)) v.toArray.filter(_ > 0.0) else v.toArray
      if (arr.length > 0) {
        val res = arr.sum / arr.length
        if (isDefined(decPlaces)) {
          val f = buildFormatter()
          val t = Try(f.format(res).toDouble)
          t.getOrElse(Double.NaN)
        } else {
          res
        }
      } else {
        0.0
      }
    }
  }

  override protected def outputDataType: DataType = DoubleType

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}", uid)
    super.transform(dataset)
  }
}

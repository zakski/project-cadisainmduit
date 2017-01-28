package com.szadowsz.ulster.spark.transformers.math.vec

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, DoubleType}
import org.slf4j.LoggerFactory

/**
  * Created on 17/01/2017.
  */
class VecSumTransformer(override val uid: String) extends UnaryTransformer[Vector, Double, VecSumTransformer] {
  protected val logger = LoggerFactory.getLogger("com.szadowsz.ulster.spark")

  def this() = this(Identifiable.randomUID("sum"))

  override protected def createTransformFunc: (Vector) => Double = {
    (v: Vector) => v.toArray.sum
  }

  override protected def outputDataType: DataType = DoubleType

  override def transform(dataset: Dataset[_]): DataFrame = {
    logger.info("Executing stage {}", uid)
    super.transform(dataset)
  }
}

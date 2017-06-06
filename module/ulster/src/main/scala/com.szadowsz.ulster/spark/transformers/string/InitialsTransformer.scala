package com.szadowsz.ulster.spark.transformers.string

import com.szadowsz.ulster.spark.transformers.OutOptUnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created on 20/04/2016.
  */
class InitialsTransformer(id: String) extends OutOptUnaryTransformer[String, InitialsTransformer](id) {

  def this() = this(Identifiable.randomUID("strInitials"))

  override protected def createTransformFunc: (String) => String = (n: String) => {
    Option(n) match {
      case Some(s) => s.charAt(0).toUpper.toString
      case None => null
    }
  }

  override protected def outputDataType: DataType = StringType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }
}

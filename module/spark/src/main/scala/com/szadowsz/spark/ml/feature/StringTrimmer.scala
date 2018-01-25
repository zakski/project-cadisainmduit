package com.szadowsz.spark.ml.feature

import com.szadowsz.spark.ml.OutOptUnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created on 15/07/2016.
  */
class StringTrimmer(id: String) extends OutOptUnaryTransformer[String, StringTrimmer](id) {

  def this() = this(Identifiable.randomUID("trim"))

  override def copy(extra: ParamMap): StringTrimmer = defaultCopy(extra)

  override protected def createTransformFunc: (String) => String = {
    (s: String) => Option(s).map(_.replaceAll("\u00A0", " ").trim).orNull
  }

  override protected def outputDataType: DataType = StringType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }
}


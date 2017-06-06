package com.szadowsz.ulster.spark.transformers.string

import com.szadowsz.ulster.spark.transformers.OutOptUnaryTransformer
import org.apache.spark.ml.param.{Param, ParamValidators}
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created on 29/05/2017.
  */
class RegexGroupExtractor(id: String) extends OutOptUnaryTransformer[String, RegexGroupExtractor](id) {

  protected val pattern: Param[String] = new Param[String](this, "pattern", "regex pattern") // TODO validate pattern


  override protected def createTransformFunc: (String) => String = {
    val regex = $(pattern).r
    (s: String) => Option(s).flatMap(regex.findFirstMatchIn(_)).map(m => m.group(1)).orNull
  }

  override protected def outputDataType: DataType = StringType
}

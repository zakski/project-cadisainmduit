package com.szadowsz.spark.ml.feature

import com.szadowsz.spark.ml.OutOptUnaryTransformer
import org.apache.spark.ml.param.Param
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

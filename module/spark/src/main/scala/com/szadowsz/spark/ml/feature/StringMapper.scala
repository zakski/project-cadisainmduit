package com.szadowsz.spark.ml.feature

import com.szadowsz.spark.ml.OutOptUnaryTransformer
import org.apache.spark.ml.param.{Param, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, StringType}

/**
  * Created on 11/07/2016.
  */
class StringMapper(id: String) extends OutOptUnaryTransformer[String, StringMapper](id) {
  protected val mode: Param[String] = new Param[String](this, "mode", "mapping mode", ParamValidators.inArray[String](Array("exact")))
  setDefault(mode,"exact")

  protected val mapping: Param[Map[String, String]] = new Param[Map[String, String]](this, "mapping", "mapping values")

  def this() = this(Identifiable.randomUID("strMap"))

  def setMode(mode: String): this.type = set("mode", mode)

  def getMode: String = $(mode)

  def setMapping(mapping: Map[String, String]): this.type = set("mapping", mapping)

  def setMapping(mapping: Set[String]): this.type = set("mapping", mapping.map(v => v -> v).toMap)

  def getMapping: Map[String, String] = $(mapping)

  override protected def createTransformFunc: (String) => String = {
    $(mode) match {
      case "exact" => (in: String) => $(mapping).get(in).orNull
    }
  }
  override protected def outputDataType: DataType = StringType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }
}

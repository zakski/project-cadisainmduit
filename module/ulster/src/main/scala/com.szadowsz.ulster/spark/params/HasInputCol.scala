package com.szadowsz.ulster.spark.params

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created on 25/04/2016.
  */
trait HasInputCol  {
  this : Params =>

  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "input Column", { s: String => s != null && s.length > 0 })


  def setInputCol(input: String): this.type = set("inputCol", input)

  def getInputCol: String = $(inputCol)

}

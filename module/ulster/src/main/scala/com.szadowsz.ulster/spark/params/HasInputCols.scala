package com.szadowsz.ulster.spark.params

import org.apache.spark.ml.param.{Param, ParamValidators, Params, StringArrayParam}

/**
  * Created on 25/01/2017.
  */
trait HasInputCols {
  this : Params =>

  protected val inputCols : Param[Array[String]] = new Param[Array[String]](this,"inputCols","",ParamValidators.arrayLengthGt[String](0))

  def setInputCol(input: Array[String]): this.type = set("inputCols", input)

  def getInputCols : Array[String] = $(inputCols)
}

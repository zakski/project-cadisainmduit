package com.szadowsz.ulster.spark.params

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created on 25/04/2016.
  */
trait HasOutputCol  {
  this : Params =>

  protected val outputCol: Param[String] = new Param[String](this, "outputCol", "output Column")


  def setOutputCol(input: String): this.type = set("outputCol", input)

  def getOutputCol: String = $(outputCol)

}

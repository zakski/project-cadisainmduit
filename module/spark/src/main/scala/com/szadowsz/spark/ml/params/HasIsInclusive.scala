package com.szadowsz.spark.ml.params

import org.apache.spark.ml.param.{Param, ParamValidators, Params}

/**
  * Created on 25/01/2017.
  */
trait HasIsInclusive  {
  this : Params =>

  protected val isInclusive : Param[Boolean] = new Param[Boolean](this,"isInclusive","")


  def checkIfInclusive : Boolean = $(isInclusive)
}

package com.szadowsz.grainne.validation


/**
  * @author zakski : 15/02/2016.
  */
abstract class AbstractValidator[T <: Serializable] extends Serializable {


  def validate(bean : T): Boolean
}

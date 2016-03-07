package com.szadowsz.grainne.validation

import com.szadowsz.grainne.data.CensusDataBean

/**
  * @author zakski : 15/02/2016.
  */
abstract class AbstractValidator extends Serializable {


  def validate(bean : CensusDataBean): Boolean
}

package com.szadowsz.grainne.staging.validation

import com.szadowsz.grainne.data.CensusDataBean
import com.szadowsz.grainne.validation.AbstractValidator

/**
  * @author Zakski : 25/03/2016.
  */
class GenderValidator extends AbstractValidator[CensusDataBean] {

  override def validate(bean: CensusDataBean): Boolean = {
    bean.getGender.isDefined
  }
}

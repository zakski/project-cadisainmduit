package com.szadowsz.grainne.staging.validation

import com.szadowsz.grainne.data.CensusDataBean
import com.szadowsz.grainne.validation.AbstractValidator

/**
  * @author Zakski : 25/03/2016.
  */
class NameValidator extends AbstractValidator[CensusDataBean] {

  override def validate(bean: CensusDataBean): Boolean = {
    bean.getSurname.length > 0 && bean.getSurname.length <= 3 && bean.getForename.length > 0 && bean.getForename.length <= 3
  }
}

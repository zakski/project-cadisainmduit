package com.szadowsz.grainne.validation

import com.szadowsz.grainne.data.CensusDataBean

/**
  * @author Zakski : 15/02/2016.
  */
class AgeValidator extends AbstractValidator {

  override def validate(bean: CensusDataBean): Boolean = {
 //   bean.getAge.exists(age => age >= 0 && age <= 125)
    true
  }
}

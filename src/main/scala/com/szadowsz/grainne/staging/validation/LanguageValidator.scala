package com.szadowsz.grainne.staging.validation

import com.szadowsz.grainne.data.CensusDataBean
import com.szadowsz.grainne.validation.AbstractValidator

/**
  * @author Zakski : 25/03/2016.
  */
class LanguageValidator extends AbstractValidator[CensusDataBean] {

  override def validate(bean: CensusDataBean): Boolean = {
    bean.getKnowsEnglish.exists(b => b) ||
      bean.getKnowsIrish.exists(b => b) ||
      bean.getIsCommonwealth.exists(b => b) ||
      bean.getAge.exists(a => a <= 5)
  }
}

package com.szadowsz.standard.cell

import com.szadowsz.standard.values.Gender
import org.supercsv.util.CsvContext

/**
 * @author Zakski : 29/07/2015.
 */
class GenderCell extends DistributionCell {

  def males = count(Gender.MALE)

  def females = count(Gender.FEMALE)

  def unknowns = count(Gender.UNKNOWN)

  def malePerCent = percentage(Gender.MALE)

  def femalePerCent = percentage(Gender.FEMALE)

  def unknownPerCent = percentage(Gender.UNKNOWN)

  override def execute(value: AnyRef, context: CsvContext): AnyRef = {
    val sex = value.asInstanceOf[String]

    sex match {
      case "M" => add(Gender.MALE)
      case "F" => add(Gender.FEMALE)
      case _ => add(Gender.UNKNOWN)
    }
  }
}

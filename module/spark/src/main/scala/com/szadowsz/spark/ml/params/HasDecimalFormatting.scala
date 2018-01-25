package com.szadowsz.spark.ml.params

import java.math.RoundingMode
import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created on 27/01/2017.
  */
trait HasDecimalFormatting {
  this : Params =>

  protected val decPlaces: Param[Int] = new Param[Int](this, "decPlaces", "number of decimal places")

  protected val roundingMode: Param[String] = new Param[String](this, "roundingMode", "the method of rounding to use")

  protected def buildFormatter():DecimalFormat = {
    val formatter = NumberFormat.getNumberInstance(Locale.ENGLISH).asInstanceOf[DecimalFormat]
    formatter.setGroupingUsed(false)
    formatter.setMaximumFractionDigits($(decPlaces))
    formatter.setMinimumFractionDigits($(decPlaces))
    formatter.setRoundingMode(RoundingMode.valueOf($(roundingMode)))
    formatter
  }

  def setDecimalPlaces(input: Int): this.type = set(decPlaces, input)

  def getDecimalPlaces: Int = $(decPlaces)

  def setRoundingMode(input: String): this.type = set(roundingMode, input)

  def getRoundingMode: String = $(roundingMode)

}

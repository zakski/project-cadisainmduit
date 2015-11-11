package com.szadowsz.census.mapping

/**
  * Utility Object to aid in calculating age banding for the census.
  *
  * Created by zakski on 11/11/2015.
  */
object AgeBanding {

  val DEFAULT_BAND_SIZE = 5

  def ageToBand(age: Int): (Int, Int) = ageToBand(age, DEFAULT_BAND_SIZE)

  def ageToBand(age: Int, bandSize: Int): (Int, Int) = {
    val bandNumber = age / bandSize
    val bandStart = bandNumber * bandSize
    (bandStart, bandStart + bandSize)
  }
}

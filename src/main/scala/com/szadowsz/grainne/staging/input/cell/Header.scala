package com.szadowsz.grainne.input.cell

/**
  * Created by zakski on 15/02/2016.
  */
object Header extends Enumeration {
  val SUR = Value("surname")
  val FORE = Value("forename")
  val TOWNLAND = Value("townlandOrStreet")
  val DISTRICT = Value("ded")
  val CO = Value("county")
  val AGE = Value("age")
  val GENDER = Value("gender")
  val BIRTH = Value("birthplace")
  val OCC = Value("occupation")
  val REL = Value("religion")
  val LIT = Value("literacy")
  val LANG = Value("knowsIrish")
  val STAT = Value("relationToHeadOfHouse")
  val SING = Value("married")
  val HEALTH = Value("illnesses")

  def getHeaders: Array[Header.Value] = {
    Array(SUR,FORE,TOWNLAND,DISTRICT,CO,AGE,GENDER,BIRTH,OCC,REL,LIT,LANG,STAT,SING,HEALTH)
  }

  def getHeadersAsString: Array[String] = {
    getHeaders.map(_.toString)
  }
}

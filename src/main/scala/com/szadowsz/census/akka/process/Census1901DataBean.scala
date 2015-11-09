package com.szadowsz.census.akka.process

import com.szadowsz.census.akka.process.cell.Gender

/**
 * Bean to hold data extracted by Super CSV
 *
 * @author Zakski : 29/07/2015.
 */
class Census1901DataBean() {
  private var _surname: String = null

  private var _forename: String = null

  private var _townlandOrStreet: String = null

  private var _ded: String = null

  private var _county: String = null

  private var _age: Int = 0

  private var _gender: Gender = Gender.UNKNOWN

  private var _birthplace: String = null

  private var _occupation: String = null

  private var _religion: String = null

  private var _literacy: String = null

  private var _knowsIrish: String = null

  private var _relationToHeadOfHouse: String = null

  private var _married: String = null

  private var _illnesses: String = null


  def setSurname(value: String): Unit = {
    _surname = value
  }

  def setForename (value: String): Unit = {
    _forename = value
  }

  def setTownlandOrStreet(value: String): Unit = {
    _townlandOrStreet = value
  }


  def setDed(value: String): Unit = {
    _ded = value
  }

  def setCounty(value: String): Unit = {
    _county = value
  }

  def setAge(value: Int): Unit = {
    _age = value
  }

  def setGender(value: Gender): Unit = {
    _gender = value
  }

  def setBirthplace(value: String): Unit = {
    _birthplace = value
  }

  def setOccupation(value: String): Unit = {
    _occupation = value
  }

  def setReligion(value: String): Unit = {
    _religion = value
  }

  def setLiteracy(value: String): Unit = {
    _literacy = value
  }

  def setKnowsIrish(value: String): Unit = {
    _knowsIrish = value
  }

  def setRelationToHeadOfHouse(value: String): Unit = {
    _relationToHeadOfHouse = value
  }
  def setMarried(value: String): Unit = {
    _married = value
  }
  def setIllnesses(value: String): Unit = {
    _illnesses = value
  }

  override def toString = _surname
}

package com.szadowsz.census

import com.szadowsz.census.mapping.{County, AgeBanding, SurnameOrigins, Gender}


/**
 * Bean to hold data extracted by Super CSV
 *
 * @author Zakski : 29/07/2015.
 */
class CensusDataBean() {
  private var _surname: Option[String] = None

  private var _surnameOrigins: Option[Set[String]] = None

  private var _forename: Option[String] = None

  private var _townlandOrStreet: Option[String] = None

  private var _ded: Option[String] = None

  private var _county: County = County.MISSING

  private var _age : Option[Int] = None

  private var _ageBanding: Option[(Int,Int)] = None

  private var _gender: Gender = Gender.MISSING

  private var _birthplace: Option[String] = None

  private var _occupation: Option[String] = None

  private var _religion: Option[String] = None

  private var _literacy: Option[String] = None

  private var _knowsIrish: Option[String] = None

  private var _relationToHeadOfHouse: Option[String] = None

  private var _married: Option[String] = None

  private var _illnesses: Option[String] = None


  def setSurname(opt : Option[String]):Unit = {
    _surname = opt
    _surnameOrigins = SurnameOrigins.getOrigins(_surname.getOrElse(""))
  }

  def setForename (opt : Option[String]):Unit = {
    _forename = opt
  }

  def setTownlandOrStreet(opt : Option[String]):Unit = {
    _townlandOrStreet = opt
  }


  def setDed(opt : Option[String]):Unit = {
    _ded = opt
  }

  def setCounty(value : County):Unit = {
    _county = value
  }

  def setAge(value: Option[Int]): Unit = {
    _age = value
    _ageBanding = _age.map(AgeBanding.ageToBand)
  }

  def setGender(value: Gender): Unit = {
    _gender = value
  }

  def setBirthplace(opt : Option[String]):Unit = {
    _birthplace = opt
  }

  def setOccupation(opt : Option[String]):Unit = {
    _occupation = opt
  }

  def setReligion(opt : Option[String]):Unit = {
    _religion = opt
  }

  def setLiteracy(opt : Option[String]):Unit = {
    _literacy = opt
  }

  def setKnowsIrish(opt : Option[String]):Unit = {
    _knowsIrish = opt
  }

  def setRelationToHeadOfHouse(opt : Option[String]):Unit = {
    _relationToHeadOfHouse = opt
  }
  def setMarried(opt : Option[String]):Unit = {
    _married = opt
  }
  def setIllnesses(opt : Option[String]):Unit = {
    _illnesses = opt
  }

  def getSurname: Option[String] = _surname

  def getSurnameOrigins: Option[Set[String]] = _surnameOrigins

  def getForename: Option[String] = _forename

  def getTownlandOrStreet: Option[String] = _townlandOrStreet

  def getDed: Option[String] =_ded

  def getCounty: County = _county

  def getAge: Option[Int] = _age

  def getAgeBanding: Option[(Int,Int)] = _ageBanding

  def getGender: Gender = _gender

  def getBirthplace: Option[String] = _birthplace

  def getOccupation: Option[String] = _occupation

  def getReligion: Option[String] = _religion

  def getLiteracy: Option[String] = _literacy

  def getKnowsIrish: Option[String] = _knowsIrish

  def getRelationToHeadOfHouse: Option[String] = _relationToHeadOfHouse

  def getMarried: Option[String] = _married

  def getIllnesses: Option[String] = _illnesses

  override def toString = _surname + "," + _surnameOrigins + "," + _age
}

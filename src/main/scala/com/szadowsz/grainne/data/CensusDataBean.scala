package com.szadowsz.grainne.data

import com.szadowsz.grainne.input.util.AgeBanding
import com.szadowsz.grainne.input.util.spelling.BirthSpell
import com.szadowsz.common.reflection.ReflectionUtil


/**
  * Bean to hold data extracted by Super CSV.
  *
  * Expected Input Columns are:
  * 1:  surname
  * 2:  forename
  * 3:  townlandOrStreet
  * 4:  ded
  * 5:  county
  * 6:  age
  * 7:  gender
  * 8:  birthplace
  * 9:  occupation
  * 10: religion
  * 11: literacy
  * 12: knowsIrish
  * 13: relationToHeadOfHouse
  * 14: married
  * 15: illnesses
  *
  * @author Zakski : 29/07/2015.
  */
class CensusDataBean extends Serializable {

  private var surnames: List[String] = Nil
  private var forenames: List[String] = Nil

  private var townlandOrStreet: Option[String] = None
  private var ded: Option[String] = None
  private var county: Option[County] = None

  private var age: Option[Int] = None
  private var ageBanding: Option[(Int, Int)] = None

  private var gender: Option[Gender] = None

  private var countyOfBirth: Option[County] = None

  private var countryOfBirth: Option[String] = None

  private var isCommonwealth : Option[Boolean] = None

  private var occupation: Option[String] = None

  private var religion: Option[String] = None

  private var canRead: Option[Boolean] = None
  private var canWrite: Option[Boolean] = None

  private var languages: List[String] = Nil

  private var relationToHeadOfHouse: Option[String] = None

  private var married: Option[String] = None

  private var illnesses: Option[String] = None


  def setSurname(opt: Option[List[String]]): Unit = surnames = opt.getOrElse(Nil)
  def setForename(opt: Option[List[String]]): Unit = forenames = opt.getOrElse(Nil)

  def setTownlandOrStreet(opt: Option[String]): Unit = townlandOrStreet = opt
  def setDed(opt: Option[String]): Unit = ded = opt
  def setCounty(value: Option[County]): Unit = county = value

  def setAge(value: Option[Int]): Unit = {
    age = value
    ageBanding = age.map(AgeBanding.ageToBand)
  }

  def setGender(value: Option[Gender]): Unit = gender = value

  def setBirthplace(input: Option[(String, String, String)]): Unit = {
    countyOfBirth = if (input.exists(_._2 != "MISSING"))input.map(t => County.fromString(t._2)) else None
    countryOfBirth = if (input.exists(_._3 != "MISSING"))input.map(t => t._3) else None
    isCommonwealth = countryOfBirth.map(BirthSpell.isCommonwealth)
  }

  def setOccupation(opt: Option[String]): Unit = occupation = opt

  def setReligion(opt: Option[String]): Unit = religion = opt

  def setLiteracy(opt: Option[String]): Unit = {
    val parsed = opt match {
      case Some("Read And Write") => Some(true, true)
      case Some("Read") => Some(true, false)
      case Some("Write") => Some(false, true)
      case Some("Illiterate") => Some(false, false)
      case Some("MISSING") => None
      case None => None
    }
    canRead = parsed.map(_._1)
    canWrite = parsed.map(_._2)
  }

  def setKnowsIrish(opt: List[String]): Unit = languages = opt

  def setRelationToHeadOfHouse(opt: Option[String]): Unit = relationToHeadOfHouse = opt

  def setMarried(opt: Option[String]): Unit = married = opt

  def setIllnesses(opt: Option[String]): Unit = illnesses = opt

  def getForename: List[String] = forenames

  def getSurname: List[String] = surnames

  def getCountyOfResidence: Option[County] = county

  def getAge: Option[Int] = age

  def getAgeGroup: Option[(Int, Int)] = ageBanding

  def getGender: Option[Gender] = gender

  def getCountyOfBirth: Option[County] = countyOfBirth

  def getCountryOfBirth: Option[String] = countryOfBirth

  def getIsCommonwealth: Option[Boolean] = isCommonwealth

  def getCanRead: Option[Boolean] = canRead

  def getCanWrite: Option[Boolean] = canWrite

  def getIsLiterate: Option[Boolean] = if (canRead.isEmpty || canWrite.isEmpty) None else canRead.map(_ && canWrite.get)

  def getReligion = religion

  def getKnowsEnglish = Some(languages.contains("English"))

  def getKnowsIrish = Some(languages.contains("Irish"))

  override def toString = ReflectionUtil.findJavaStyleGetters(getClass).filter(_.getName != "getClass").map(_.invoke(this).asInstanceOf[Any].toString).mkString(",")
}

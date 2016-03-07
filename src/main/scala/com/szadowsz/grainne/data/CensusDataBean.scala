package com.szadowsz.grainne.data

import com.szadowsz.grainne.input.util.AgeBanding


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


  private var _canRead: Option[Boolean] = None

  private var _canWrite: Option[Boolean] = None


  /**
    * The family names of the person.
    *
    * @note input index 1
    *
    */
  private var _surnames: List[String] = Nil

  /**
    * The name determined to be the first given name.
    *
    * @note input index 2
    *
    */
  private var _forename: Option[String] = None


  /**
    * The subsequent given names of the person, if any.
    *
    * @note input index 2 (originally part of the forename)
    *
    */
  private var _middlenames: List[String] = Nil

  /**
    * The townlands index of the person's residence.
    *
    * @see http://www.irishancestors.ie/?page_id=5392
    * @note input index 3
    *
    */
  private var _townlandOrStreet: Option[String] = None

  /**
    * The district electoral division of the person's residence.
    *
    * @see http://www.irishancestors.ie/?page_id=5392
    * @note input index 4
    *
    */
  private var _ded: Option[String] = None

  /**
    * The County of the person's residence.
    *
    * @note input index 5
    *
    */
  private var _county: Option[County] = None

  /**
    * The Age of the person.
    *
    * @note input index 6
    *
    */
  private var _age: Option[Int] = None

  /**
    * The Gender of the person.
    *
    * @note input index 7
    *
    */
  private var _gender: Option[Gender] = None

  private var _birthplace: Option[String] = None

  private var _occupation: Option[String] = None

  private var _religion: Option[String] = None

  private var _languages: Option[String] = None

  private var _relationToHeadOfHouse: Option[String] = None

  private var _married: Option[String] = None

  private var _illnesses: Option[String] = None

  private var _ageBanding: Option[(Int, Int)] = None
  private var _surnameOrigins: Option[Set[String]] = None
  private var _middlenamesOrigins: Option[List[Option[Set[String]]]] = None
  private var _forenameOrigins: Option[Set[String]] = None


  def setSurname(opt: Option[List[String]]): Unit = {
    _surnames = opt.getOrElse(Nil)
    //   _surnameOrigins = SurnameOrigins.getOrigins(_surname.getOrElse(""))
  }

  def setForename(opt: Option[List[String]]): Unit = {
    _forename = opt.map(_.head)
    _middlenames = opt.map(_.tail).getOrElse(Nil)
    //  _forenameOrigins = FirstnameOrigins.getOrigins(_forename.getOrElse(""))
    //   _middlenamesOrigins = _middlenames.map(l => l.map(s => FirstnameOrigins.getOrigins(s)))
  }

  def setTownlandOrStreet(opt: Option[String]): Unit = {
    _townlandOrStreet = opt
  }


  def setDed(opt: Option[String]): Unit = {
    _ded = opt
  }

  def setCounty(value: Option[County]): Unit = {
    _county = value
  }

  def setAge(value: Option[Int]): Unit = {
    _age = value
    _ageBanding = _age.map(AgeBanding.ageToBand)
  }

  def setGender(value: Option[Gender]): Unit = {
    _gender = value
  }

  def setBirthplace(opt: Option[String]): Unit = {
    _birthplace = opt
  }

  def setOccupation(opt: Option[String]): Unit = {
    _occupation = opt
  }

  def setReligion(opt: Option[String]): Unit = {
    _religion = opt
  }

  def setLiteracy(opt: Option[String]): Unit = {
    val parsed = opt match {
      case Some("Read And Write") => Some(true, true)
      case Some("Read") => Some(true, false)
      case Some("Write") => Some(false, true)
      case Some("Illiterate") => Some(false, false)
      case Some("MISSING") => None
      case None => None
    }

    _canRead = parsed.map(_._1)
    _canWrite = parsed.map(_._2)
  }

  def setKnowsIrish(opt: Option[String]): Unit = {
    _languages = opt
  }

  def setRelationToHeadOfHouse(opt: Option[String]): Unit = {
    _relationToHeadOfHouse = opt
  }

  def setMarried(opt: Option[String]): Unit = {
    _married = opt
  }

  def setIllnesses(opt: Option[String]): Unit = {
    _illnesses = opt
  }

  def getCounty: Option[County] = _county

  def getAge: Option[Int] = _age

  def getGender: Option[Gender] = _gender

  def getCanRead: Option[Boolean] = _canRead

  def getCanWrite: Option[Boolean] = _canWrite

  def getLiteracy:Option[Boolean] = if(_canRead.isEmpty || _canWrite.isEmpty) None else _canRead.map(_ && _canWrite.get)

  //  def getSurnames: List[String] = _surnames
  //
  // // def getSurnameOrigins: Option[Set[String]] = _surnameOrigins
  //
  //  def getForename: Option[String] = _forename
  //
  // // def getForenameOrigins: Option[Set[String]] = _forenameOrigins
  //
  //  def getMiddlenames: List[String] = _middlenames
  //
  //  def getTownlandOrStreet: Option[String] = _townlandOrStreet
  //
  //  def getDed: Option[String] = _ded
  //

  //
  //  def getAgeBanding: Option[(Int, Int)] = _ageBanding
  //

  //
  //  def getBirthplace: Option[String] = _birthplace
  //
  //  def getOccupation: Option[String] = _occupation
  //
  //  def getReligion: Option[String] = _religion

  def getLanguages: Option[String] = _languages

  //
  //  def getRelationToHeadOfHouse: Option[String] = _relationToHeadOfHouse
  //
  //  def getMarried: Option[String] = _married
  //
  //  def getIllnesses: Option[String] = _illnesses

  override def toString = _surnames + "," + _surnameOrigins + "," + _age
}

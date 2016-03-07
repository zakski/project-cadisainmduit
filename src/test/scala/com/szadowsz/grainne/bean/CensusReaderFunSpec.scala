package com.szadowsz.grainne.bean

import com.szadowsz.grainne.data.{Language, Gender, County}
import com.szadowsz.grainne.input.cell.{Header, CensusReader, SparkCell}
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

/**
  * Gamble,Thomas,Kinallen,Skeagh,Down,29,M,County Down,Labourer,Presbyterian,Read and write,English,Head of Family,Married,
  *
  * Created by zakski on 24/11/2015.
  */
@RunWith(classOf[JUnitRunner])
class CensusReaderFunSpec extends FunSpec {

//  val reader = CensusReader(Header.getHeadersAsString, Header.getHeaders.map(SparkCell(_)))
//
//  describe("A Census Reader Instance") {
//
//    it("should read empty data correctly") {
//      val vals = ",,,,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//
//      assertResult(None)(result.getSurnames)
//      assertResult(None)(result.getForename)
//      assertResult(None)(result.getMiddlenames)
//      assertResult(None)(result.getTownlandOrStreet)
//      assertResult(None)(result.getDed)
//      assertResult(None)(result.getCounty)
//      assertResult(None)(result.getAge)
//      assertResult(None)(result.getGender)
//      assertResult(None)(result.getBirthplace)
//      assertResult(None)(result.getOccupation)
//      assertResult(None)(result.getReligion)
//      assertResult(None)(result.getLiteracy)
//      assertResult(List(Language.ENGLISH))(result.getKnowsIrish)
//      assertResult(None)(result.getRelationToHeadOfHouse)
//      assertResult(None)(result.getMarried)
//      assertResult(None)(result.getIllnesses)
//    }
//
//    it("should read a single surname correctly") {
//      val vals = "Murphy,,,,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(List("Murphy")))(result.getSurnames)
//    }
//
//    it("should read a single forename correctly") {
//      val vals = ",Sean,,,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Sean"))(result.getForename)
//    }
//
//    it("should read multiple given names correctly") {
//      val vals = ",Lucas Sean-Charles,,,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Lucas"))(result.getForename)
//      assertResult(Some(List("Sean","Charles")))(result.getMiddlenames)
//    }
//
//    it("should read the townland/street name correctly") {
//      val vals = ",,Shankill Road,,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Shankill Road"))(result.getTownlandOrStreet)
//     }
//
//    it("should read the district electoral division name correctly") {
//      val vals = ",,,Mountjoy,,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Mountjoy"))(result.getDed)
//    }
//
//    it("should parse Londonderry correctly") {
//      val vals = ",,,,Londonderry,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(County.DERRY))(result.getCounty)
//    }
//
//    it("should parse Laois correctly") {
//      val vals = ",,,,Queen's Co.,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(County.LAOIS))(result.getCounty)
//    }
//
//    it("should parse Offaly correctly") {
//      val vals = ",,,,King's Co.,,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(County.OFFALY))(result.getCounty)
//    }
//
//    it("should parse Age correctly") {
//      val vals = ",,,,,25,,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(25))(result.getAge)
//    }
//
//    it("should parse Males correctly") {
//      val vals = ",,,,,,M,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(Gender.MALE))(result.getGender)
//    }
//
//    it("should parse Females correctly") {
//      val vals = ",,,,,,F,,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some(Gender.FEMALE))(result.getGender)
//    }
//
//    it("should read Birthplace correctly") {
//      val vals = ",,,,,,,England,,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("England"))(result.getBirthplace)
//    }
//
//    it("should read Occupation correctly") {
//      val vals = ",,,,,,,,Carpenter,,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Carpenter"))(result.getOccupation)
//    }
//
//    it("should parse Religion correctly") {
//      val vals = ",,,,,,,,,Roman Catholic,,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Roman Catholic"))(result.getReligion)
//    }
//
//    it("should parse Literacy correctly") {
//      val vals = ",,,,,,,,,,Read and write,,,,"
//
//      val result = reader.read(vals)
//      assertResult(Some("Read and write"))(result.getLiteracy)
//    }
//
//    it("should parse multiple Languages correctly") {
//      val vals = ",,,,,,,,,,,Irish and English,,,"
//
//      val result = reader.read(vals)
//      assertResult(List(Language.ENGLISH,Language.IRISH))(result.getKnowsIrish)
//    }
//
//    it("should parse negative Irish sentiment correctly") {
//      val vals = ",,,,,,,,,,,Can't speak Irish,,,"
//
//      val result = reader.read(vals)
//      assertResult(List(Language.ENGLISH))(result.getKnowsIrish)
//    }
//  }
}

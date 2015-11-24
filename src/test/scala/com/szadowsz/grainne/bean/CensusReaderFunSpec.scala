package com.szadowsz.grainne.bean

import com.szadowsz.grainne.stats.ColumnStatistics
import org.scalatest.FunSpec

/**
  * Gamble,Thomas,Kinallen,Skeagh,Down,29,M,County Down,Labourer,Presbyterian,Read and write,English,Head of Family,Married,
  *
  * Created by zakski on 24/11/2015.
  */
class CensusReaderFunSpec extends FunSpec {

  describe("A Census Reader Instance") {

    it("should read empty data") {
      val vals = ",,,,,,,,,,,,,,"
      val reader = CensusReader(CensusReader.HEADERS_1901,CensusReader.CELLS_1901)

      val result = reader.read(vals)
      assertResult(None)(result.getSurname)
      assertResult(None)(result.getForename)
    }
  }
}

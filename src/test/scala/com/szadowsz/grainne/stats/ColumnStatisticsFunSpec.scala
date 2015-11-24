package com.szadowsz.grainne.stats

import com.szadowsz.grainne.stats.ColumnStatistics
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnStatisticsFunSpec extends FunSpec {

  describe("A Row Statistics Instance") {

    it("should order values from low to high for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.lowToHigh
      assertResult("B")(result.head._1)
    }

    it("should order values from high to low for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.highToLow
      assertResult("C")(result.head._1)
    }

    it("should calculate the median for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.median
      assertResult("A")(result._1)
    }

    it("should calculate the average for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.average
      assertResult(2.0)(result)
    }

    it("should calculate the variance for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.variance
      assertResult(2.0/3.0)(result)
    }

    it("should calculate the standard deviation for non-empty data") {
      val vals = List("A", "A", "B", "C", "C", "C")
      val stats = new ColumnStatistics[String]()
      stats.add(vals: _*)

      val result = stats.stdDeviation
      assertResult(Math.sqrt(2.0/3.0))(result)
    }
  }
}
package com.szadowsz.grainne.stats

/**
  * Created by zakski on 24/11/2015.
  */
class ColStats[T <: Any](val id : String, private val dataSource : Map[T,Long]) {


  /**
    * Map of values to how many times they have occurred in the data.
    */
  protected var normSource: Map[T, Double] = calcPercentages


  /**
    * Method to include new data for the purpose of statistics calculation.
    */
  private def calcPercentages(): Map[T, Double] = {
    val count = dataSource.values.sum.toDouble
    dataSource.map(d => d._1 -> (d._2.toDouble / count)).toMap
  }

  def lowToHigh: List[(T, Long)] = dataSource.toList.sortBy{ case (key, value) => (value, key.toString) }

  def highToLow: List[(T, Long)] = dataSource.toList.sortBy{ case (key, value) => (-value, key.toString) }

  def median: (T, Long) = {
    lowToHigh(dataSource.size / 2)
  }

  def average: Double = dataSource.values.sum.toDouble / dataSource.size.toDouble

  def deviations: Map[T, Double] = {
    val av = average
    dataSource.map(d => d._1 -> (d._2 - av)).toMap
  }

  def variance: Double = {
    deviations.map(d => (d._1, d._2 * d._2)).values.sum / dataSource.size.toDouble
  }

  def stdDeviation: Double = {
    Math.sqrt(variance)
  }
}

package com.szadowsz.grainne.stats

object ColStats {


  def apply[T] (id : String, data : Map[T,Long]) = new ColStats[T](id,data)

}

/**
  * Created by zakski on 24/11/2015.
  */
class ColStats[T <: Any](val id : String, private val dataSource : Map[T,Long]) {


  /**
    * Map of values to how many times they have occurred in the data.
    */
  protected var normSource: Map[T, Double] = calcPercentages


  /**
    * Ordering to sort values by their occurrence.
    */
  protected val order: Ordering[(T, Long)] = Ordering.by(_._2)

  /**
    * Method to include new data for the purpose of statistics calculation.
    */
  private def calcPercentages(): Map[T, Double] = {
    val count = dataSource.values.sum.toDouble
    dataSource.map(d => d._1 -> (d._2.toDouble / count)).toMap
  }

  def lowToHigh: List[(T, Long)] = dataSource.toList.sorted(order)

  def highToLow: List[(T, Long)] = dataSource.toList.sorted(order.reverse)

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

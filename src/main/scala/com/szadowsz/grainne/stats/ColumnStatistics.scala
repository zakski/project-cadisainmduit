package com.szadowsz.grainne.stats

import scala.collection.{mutable => mut}


object ColumnStatistics {

  def apply[T <: AnyRef]: ColumnStatistics[T] = new ColumnStatistics[T]()
}

/**
  * Created by zakski on 24/11/2015.
  */
class ColumnStatistics[T <: AnyRef] {

  /**
    * Map of values to how many times they have occurred in the data.
    */
  protected val dataSource: mut.Map[T, Int] = mut.Map()

  /**
    * Map of values to how many times they have occurred in the data.
    */
  protected var normSource: Map[T, Double] = Map()


  /**
    * Ordering to sort values by their occurrence.
    */
  protected val order: Ordering[(T, Int)] = Ordering.by(_._2)

  /**
    * Method to include new data for the purpose of statistics calculation.
    *
    * @param data data to add to the statistics.
    */
  def add(data: T*): Unit = {
    data.foreach(d => dataSource.update(d, dataSource.getOrElse(d, 0) + 1))
    val count = dataSource.values.sum.toDouble
    normSource = dataSource.map(d => d._1 -> (d._2.toDouble / count)).toMap
  }

  def lowToHigh: List[(T, Int)] = dataSource.toList.sorted(order)

  def highToLow: List[(T, Int)] = dataSource.toList.sorted(order.reverse)

  def median: (T, Int) = {
    lowToHigh(dataSource.size / 2)
  }

  def average: Double = dataSource.values.sum.toDouble / dataSource.size.toDouble

  def deviations: mut.Map[T, Double] = {
    val av = average
    dataSource.map(d => d._1 -> (d._2 - av))
  }

  def variance: Double = {
    deviations.map(d => (d._1, d._2 * d._2)).values.sum / dataSource.size.toDouble
  }

  def stdDeviation: Double = {
    Math.sqrt(variance)
  }
}

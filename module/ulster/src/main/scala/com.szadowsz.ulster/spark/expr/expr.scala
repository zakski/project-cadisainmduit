package com.szadowsz.ulster.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created on 29/04/2016.
  */
package object expr {

  implicit class SchemaUtil(val schema: StructType) {

    def fieldIndexOf(col : String):Int = schema.fieldNames.indexOf(col)

    def get(col : String): Option [StructField] = schema.find(_.name == col)
  }

  implicit private[szadowsz] class DataFrameUtil(val dataset: DataFrame) {

    /**
      * Method to count the number of rows that have each category.
      *
      * @param head required column name
      * @param tail optional additional column names
      * @return aggregated count of distinct categories
      */
    def countDistinct(head: String, tail: String*): Array[Map[Any, Long]] = {
      val nominalData = dataset.select(head, tail: _*)
      val length = nominalData.schema.fields.length

      // count the categories per partition
      val summaryData = nominalData.rdd.mapPartitions(rows => {
        val maps = Array.fill(length)(Map[Any, Long]())
        rows.foreach(row => {
          row.toSeq.zipWithIndex.foreach {
            case (field, index) => if (field != null) {
              val tmp = maps(index)
              maps(index) = tmp + (field -> (tmp.getOrElse(field, 0L) + 1L))
            }
          }
        })
        List(maps).toIterator
      }, false)

      // aggregate the partitions into an array of maps
      summaryData.reduce { case (a1, a2) =>
        a1.zip(a2).map { case (m1, m2) =>
          m1.foldLeft(m2) { case (m, (k, v)) =>
            val n = m.getOrElse(k, 0L) + v
            m + (k -> n)
          }
        }
      }
    }
  }
}

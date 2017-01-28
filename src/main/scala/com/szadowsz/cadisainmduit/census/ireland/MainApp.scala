package com.szadowsz.cadisainmduit.census.ireland

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created on 28/11/2016.
  */
object MainApp {

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val stringRdd = sess.sparkContext.textFile("./data/census/ireland/1901/census-[0-9]*.csv")
    stringRdd.cache()
    require(stringRdd.count() == 4429866)

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache()

    val pipe = RecipeUtils.buildWords//buildMain

    val model = pipe.fit(df)
    val result = model.transform(df)
  }
}

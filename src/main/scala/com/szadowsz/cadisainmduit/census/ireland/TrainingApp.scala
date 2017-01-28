package com.szadowsz.cadisainmduit.census.ireland

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created on 28/11/2016.
  */
object TrainingApp {

  def main(args: Array[String]): Unit = {
    val sess = SparkSession.builder()
      .config("spark.driver.host", "localhost")
      .master("local[4]")
      .getOrCreate()

    val stringRdd = sess.sparkContext.textFile("./data/debug/1901-9/religion.csv",4)
    stringRdd.cache()

    val rowRDD = stringRdd.map(s => Row.fromSeq(List(s)))
    val df = sess.createDataFrame(rowRDD, StructType(Array(StructField("fields", StringType))))
    df.cache()

    val pipe = RecipeUtils.buildRelMappingOptimisation

    val model = pipe.fit(df)
    val result = model.transform(df)
  }
}

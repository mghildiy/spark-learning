package com.manish.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object BasicExample {

  def main(args: Array[String]) = {
    val sparkSession = SparkSession
      .builder()
      .appName("Basic Example")
      .config("spark.master", "local")
      .getOrCreate()
    // Create a DataFrame of names and ages
    val dataFrame = sparkSession.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")
    // Group the same names together, aggregate their ages, and compute an average
    val avgDF = dataFrame.groupBy("name").agg(avg("age"))
    // Show the results of the final execution
    avgDF.show()
  }

}

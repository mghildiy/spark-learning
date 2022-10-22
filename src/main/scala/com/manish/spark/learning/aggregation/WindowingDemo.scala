package com.manish.spark.learning.aggregation

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowingDemo extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Windowing Demo")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val invoicesGroupedByCountryAndYWeekDF = spark
    .read
    .parquet("src/main/resources/data/sink/parquet")
    //.show(5)

  // define windowing object
  val runningInvoiceTotal = Window
    .partitionBy("Country")
    .orderBy("Week")
    .rowsBetween(-2/*Window.unboundedPreceding*/, Window.currentRow)
  invoicesGroupedByCountryAndYWeekDF
    .withColumn("RunningTotal",
      sum("TotalInvoiceAmount").over(runningInvoiceTotal)
    )
    .show()

  spark.stop

}

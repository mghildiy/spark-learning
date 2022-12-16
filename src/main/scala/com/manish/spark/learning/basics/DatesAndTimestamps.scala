package com.manish.spark.learning.basics

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, lit}

object DatesAndTimestamps extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = Utils.createSparkSession("Dates And Timestamps")
  val datesDF = spark
    .range(10)
    .withColumn("Today", expr("current_date()"))
    .withColumn("Now", expr("current_timestamp()"))
  //datesDF.show()

  datesDF
    .select(expr("Today"),expr("date_sub(Today, 5)").as("5DaysBack"), expr("date_add(Today, 5)").as("5DaysAhead"))
    //.show()

  val stringDates = spark
    .range(10)
    .withColumn("StringDate", lit("2022-14-01"))
  //stringDates.show()
  val dateFormat = "yyyy-dd-MM"
  stringDates
    .withColumn("Date", expr(s"to_date(StringDate, '$dateFormat')"))
    .withColumn("TimeStamp", expr("to_timestamp(StringDate, 'yyyy-dd-MM')"))
    .show()

  spark.stop()

}

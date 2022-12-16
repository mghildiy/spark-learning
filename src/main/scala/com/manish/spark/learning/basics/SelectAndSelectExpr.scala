package com.manish.spark.learning.basics

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object SelectAndSelectExpr extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)


  val spark = SparkSession
    .builder()
    .appName("Select And SelectExpr")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val df = spark
    .read
    .format("csv")
    .option("header", "true")
    .load("src/main/resources/flight-data-2015.csv")
    .toDF("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME", "count")

  df.createOrReplaceTempView("flight_data_2015")

  // sql SELECT
  df.select(col("DEST_COUNTRY_NAME")).show(2)
  df.select(column("DEST_COUNTRY_NAME")).show(2)
  df.select("DEST_COUNTRY_NAME").show(2)
  df.select(df.col("DEST_COUNTRY_NAME")).show(2)
  //import spark.implicits._
  //df.select($"").show(2)
  df.select(expr("DEST_COUNTRY_NAME")).show(2)

  df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
  df.select(expr("DEST_COUNTRY_NAME AS destination").alias("REVERT_TO_DEST_COUNTRY_NAME")).show(2)
  df.selectExpr("DEST_COUNTRY_NAME as destination", "ORIGIN_COUNTRY_NAME").show(2)
  df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(false)
  df.withColumn("WithinCountry", expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME")).show(2)
  df.drop(expr("count")).show(2)
  df.withColumn("count", expr("count as long")).show(2)
  df.filter(expr("count < 2")).show(2)
  df.filter(col("count") < 2).show(2)
  df.where(expr("count < 2")).show(2)
  df.where(col("count") < 2).show(2)
  df.where(expr("count < 2 AND ORIGIN_COUNTRY_NAME <> 'CROATIA'")).show(2)
  df.select(expr("ORIGIN_COUNTRY_NAME")).distinct().show()
  df
    .orderBy(expr("DEST_COUNTRY_NAME").desc, expr("count").asc)
    .drop(expr("ORIGIN_COUNTRY_NAME"))
    .show(25)
  df.sort(expr("DEST_COUNTRY_NAME").desc, expr("count").asc).show()

  spark.stop()

}

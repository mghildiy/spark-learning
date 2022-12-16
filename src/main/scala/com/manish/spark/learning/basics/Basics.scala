package com.manish.spark.learning.basics

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Basics extends App with Serializable {
  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder()
    .appName("Basics")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val ordersList = List(
    ("01", "02", 350, 1),
    ("01", "04", 580, 1),
    ("01", "07", 320, 2),
    ("02", "03", 450, 1),
    ("02", "06", 220, 1),
    ("03", "01", 195, 1),
    ("04", "09", 270, 3),
    ("04", "08", 410, 2),
    ("05", "02", 350, 1)
  )
  val df: Dataset[Row] = spark.createDataFrame(ordersList)
  println(df.schema)
  val dfWithCustomCols: Dataset[Row] = df.toDF("order_id", "prod_id", "unit_price", "qty")
  println(dfWithCustomCols.schema)
  df.foreach(record => {
    println(record.getAs("_1"))
  })
  println("******************")
  dfWithCustomCols.foreach(record => {
    println(record.getAs("order_id"))
  })

  col("")

  spark.stop()

}

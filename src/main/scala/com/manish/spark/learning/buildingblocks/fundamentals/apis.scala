package com.manish.spark.learning.buildingblocks.fundamentals

import com.manish.spark.learning.utils.Utils

object apis extends App {

  val spark = Utils.createSparkSession("Structured APIs")

  spark.sparkContext.setLogLevel("ERROR")

  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("D:\\work\\learning\\dataengg\\spark\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\*.csv")
  /*val schema = staticDataFrame.schema
  println(schema.fieldNames)*/

  spark.close()
}

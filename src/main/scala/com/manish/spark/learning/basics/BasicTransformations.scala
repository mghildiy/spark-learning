package com.manish.spark.learning.basics

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr

object BasicTransformations extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  Utils.createSparkSession("Data Types")
  val spark = Utils.createSparkSession("Data Types")
  val sourceDF = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", true)
    .load("src\\main\\resources\\data\\source\\retail-data\\day-by-day\\2010-12-01.csv")
  println(sourceDF.schema)
  //sourceDF.show(2)
  sourceDF.createOrReplaceTempView("retail_data")

  sourceDF
    .where(expr("InvoiceNo = 536365"))
    .select(expr("InvoiceNo"), expr("Description"))
    //.show(5, false)

  val dotHasStockCode = expr("'DOT' LIKE CONCAT('%', StockCode, '%')")
  val priceFilter = expr("UnitPrice > 600")
  val descriptionFilter = expr("Description like '%POSTAGE%'")
  sourceDF
    .where(dotHasStockCode)
    .where(priceFilter || descriptionFilter)
    //.show()

  val dotEqualsStockCode = expr("StockCode == 'DOT'")
  sourceDF
    .withColumn("isExpensive", dotEqualsStockCode && (priceFilter || descriptionFilter))
    .where(expr("isExpensive"))
    //.show()

  val newQuantity = expr("(POWER(Quantity * UnitPrice, 2) + 5) as RealQuantity")
  sourceDF
    .select(expr("CustomerID"), newQuantity)
    //.show()

  val rounded = expr("ROUND(UnitPrice) as Rounded")
  sourceDF
    .select(expr("UnitPrice"), rounded)
    //.show()

  val correlation = expr("CORR(Quantity, UnitPrice) as Correlation")
  sourceDF
    .select(correlation)
    //.show()

  val initCap = expr("INITCAP(Description)")
  sourceDF
    .select(expr("Description"), initCap)
    //.show()

  val lowerCase = expr("LOWER(Description) AS AllLower")
  val upperCase = expr("UPPER(Description) AS AllUpper")
  sourceDF
    .select(expr("Description"), lowerCase, upperCase)
    //.show()

  spark.stop()

}

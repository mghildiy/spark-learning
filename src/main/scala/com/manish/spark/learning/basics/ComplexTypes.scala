package com.manish.spark.learning.basics

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr

object ComplexTypes extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = Utils.createSparkSession("Complex types")
  val sourceDF = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", true)
    .load("src\\main\\resources\\data\\source\\retail-data\\day-by-day\\2010-12-01.csv")
  //sourceDF.show()

  sourceDF
    .select(expr("STRUCT(Description, InvoiceNo) as Complex"), expr("*"))
    .select(expr("complex"), expr("complex.*"), expr("complex.Description"), expr("complex.InvoiceNo"))
    //.show(truncate = false)

  sourceDF
    .select(expr("Description"), expr("SPLIT(Description, ' ') as Array"))
    //.show(2, false)

  sourceDF
    .select(expr("Description"), expr("SPLIT(Description, ' ') as Array"))
    .select(expr("Array[0]"))
    //.show(2, false)

  sourceDF
    .select(expr("Description"), expr("SIZE(SPLIT(Description, ' ')) as NumWordsInDescription"))
    //.show(2, false)

  sourceDF
    .select(expr("Description"), expr("array_contains(SPLIT(Description, ' '), 'WHITE') as ContainsWhite"))
    //.show(5, false)

  sourceDF
    .withColumn("Splitted", expr("SPLIT(Description , ' ')"))
    .withColumn("Exploded", expr("EXPLODE(Splitted)"))
    .show(truncate = false)

  spark.stop()

}

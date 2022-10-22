package com.manish.spark.learning.transformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UDF extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("UDF Demo")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val surveyDF = spark
    .read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("src/main/resources/data/source/survey.csv")
  //surveyDF.show()

  import org.apache.spark.sql.functions._
  val parseGenderUDF = udf(parseGender(_: String): String)
  surveyDF
    .withColumn("Gender", parseGenderUDF(column("Gender")))
    .show()

  spark.udf.register("parseGenderUDF", parseGender(_:String):String)
  surveyDF
    .withColumn("Gender", expr("parseGenderUDF(Gender)"))
    .show()

  def parseGender(s:String):String = {
    val femalePattern = "^f$|f.m|w.m".r
    val malePattern = "^m$|ma|m.l".r

    if(femalePattern.findFirstIn(s.toLowerCase).nonEmpty) "Female"
    else if (malePattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"
  }

  spark.stop()
}

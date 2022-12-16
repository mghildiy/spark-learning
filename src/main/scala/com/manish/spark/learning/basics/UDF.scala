package com.manish.spark.learning.basics

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.expr

object UDF extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = Utils.createSparkSession("UDFs")

  val numDF = spark
    .range(10)
    .toDF("Number")
  numDF.show()

  def power3(input: Int) = input * input * input

  import org.apache.spark.sql.functions.udf
  val power3UDF = udf(power3(_:Int):Int)
  spark.udf.register("power3", power3(_:Int):Int)

  numDF
    .select(expr("Number"), expr("power3(Number) AS RaisedTo3"))
    //.select(expr("Number"), power3UDF(col("Number")).as("Raised To 3"))
    .show()

}

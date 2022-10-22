package com.manish.spark.learning.transformations

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object WorkingWithRow extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Row Demo")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val mySchema = StructType(
    List(
      StructField("ID", StringType),
      StructField("EventDate", StringType))
  )
  val rows = List(Row("101", "04/05/2020"), Row("102", "4/5/2020"), Row("103", "4/05/2020"))
  val rdd = spark.sparkContext.parallelize(rows, 2)
  val df = spark.createDataFrame(rdd, mySchema)
  println(df.schema)
  df.show()
  val dateDF = toDateDF(df, "EventDate", "m/d/y")
  println(dateDF.schema)
  dateDF.show()

  spark.stop()

  def toDateDF(df: DataFrame, field: String, fmt: String) = {
    df.withColumn(field, to_date(col(field), fmt))
  }

}

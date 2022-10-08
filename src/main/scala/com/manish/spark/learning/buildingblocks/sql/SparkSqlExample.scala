package com.manish.spark.learning.buildingblocks.sql

import com.manish.spark.learning.utils.{CSV, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSqlExample {
  val spark = Utils.createSparkSession("SparkSqlExample")
  val csvFile = "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\departuredelays.csv"

  def main(args: Array[String]) = {
    Logger.getRootLogger().setLevel(Level.ERROR)

    val schema = StructType(
      Array(
        StructField("date", StringType),
        StructField("delay", IntegerType),
        StructField("distance", IntegerType),
        StructField("origin", StringType),
        StructField("destination", StringType)
      )
    )
    val departureDelaysDF = Utils.dataFrameFromSchemaAndFile(spark, schema, csvFile, CSV)

    // create a temporary view, its lifetime is tied to spark session used to create it
    departureDelaysDF.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql("SELECT distance, origin, destination" +
      " FROM us_delay_flights_tbl " +
      " WHERE" +
      " distance > 1000" +
      " ORDER BY distance DESC")
    //.show(10)

    /*spark.sql("SELECT date, delay, distance, origin, destination" +
      " FROM us_delay_flights_tbl" +
      " WHERE" +
      " delay > 120 AND origin = 'SFO' AND destination = 'ORD'" +
      " ORDER BY delay DESC")
      .withColumn("formatted date", date_format(to_timestamp(col("date"), "MMddHHmm"), "MM/dd hh:mm"))
      .drop("date")
      .show(10)*/

    spark.sql("SELECT delay, origin, destination," +
      " CASE" +
      "     WHEN delay > 360 THEN 'Very Long Delays'" +
      "     WHEN delay > 120 AND delay < 360 THEN 'Long Delays'" +
      "     WHEN delay > 60 AND delay < 120 THEN 'Short Delays'" +
      "     WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'" +
      "     WHEN delay = 0 THEN 'No Delays'" +
      "     ELSE 'Early'" +
      " END" +
      " AS Flight_Delays" +
      " FROM us_delay_flights_tbl" +
      " ORDER BY origin, delay DESC")
    //.show()

    // create database, and use it for any further table creation operations
    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    // create a managed table
    //spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT,distance INT, origin STRING, destination STRING)")
    departureDelaysDF
      .write
      //.format("parquet")
      //.mode(SaveMode.Overwrite)
      .saveAsTable("managed_us_delay_flights_tbl")

    // creating views
    spark
      .sql("SELECT date, delay, origin, destination FROM managed_us_delay_flights_tbl WHERE origin = 'SFO'")
      .createOrReplaceTempView("us_origin_airport_sfo_temp_view")
    departureDelaysDF
      .where(col("origin") === "JFK")
      .createOrReplaceGlobalTempView("us_origin_airport_jfk_global_temp_view")
    // reading from views is same as from table
    spark
      .read
      .table("us_origin_airport_sfo_temp_view")
      .show()
    spark
      .sql("SELECT * FROM global_temp.us_origin_airport_jfk_global_temp_view")
      .show()
    // drop views
    spark
      .sql("DROP VIEW IF EXISTS us_origin_airport_sfo_temp_view")
    spark
      .catalog
      .dropGlobalTempView("us_origin_airport_jfk_global_temp_view")
    // drop table
    spark
      .sql("DROP TABLE IF EXISTS managed_us_delay_flights_tbl")

    spark.close()
  }

}

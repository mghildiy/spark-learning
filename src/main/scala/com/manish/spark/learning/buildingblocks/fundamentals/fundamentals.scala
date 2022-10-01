package com.manish.spark.learning.buildingblocks.fundamentals

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{desc, max}

object fundamentals  extends App with Serializable {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  logger.info("Creating spark session")
  // access SparkSession object, only one SparkSession per main method
  val spark = Utils.createSparkSession("Fundamentals")

  spark.sparkContext.setLogLevel("ERROR")

  val ds = spark.range(1000)
  //ds.show(5)
  //ds.where("id % 2 == 0").show()
  val df = ds.toDF("number")
  //df.show(5)
  df
    .where("number % 2 == 0")
    //.show()

  //println(spark.sparkContext.defaultParallelism)

  val departureDelays = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\departuredelays.csv")

  // configure
  //spark.conf.set("spark.executor.instances", 4)
  //spark.conf.set("spark.executor.cores", 4)
  spark.conf.set("spark.sql.shuffle.partitions", "5")

  //departureDelays.take(200).foreach(println(_))
  //departureDelays.sort("delay").explain()
  //departureDelays.sort("delay").take(200).foreach(println(_))

  // registering a table using a dataframe..its lifetime is same as sparksession's used to create it
  departureDelays.createOrReplaceTempView("departure_delays")

  val sqlGroupBy = spark
    .sql("SELECT origin, count(1) from departure_delays GROUP BY origin")
  val dfGroupBy = departureDelays
    .groupBy("origin")
    .count()
  //sqlGroupBy.explain()
  //dfGroupBy.explain()
  //sqlGroupBy.show()
  //dfGroupBy.show()

  spark
    .sql("SELECT max(delay) from departure_delays")
    //.show()
  departureDelays
    .select(max("delay"))
    //.show()

  spark
    .sql("SELECT origin, max(delay) FROM departure_delays GROUP BY origin")
    .show()
  departureDelays
    .groupBy("origin")
    .max("delay")
    .show()

  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\flight-data-2015.csv")
  flightData2015.createOrReplaceTempView("flight_data_2015")


  spark
    .sql("""
        SELECT DEST_COUNTRY_NAME, sum(count) as total_flights_from
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY total_flights_from DESC
        LIMIT 5
      """)
    .show()
  flightData2015
    .groupBy("DEST_COUNTRY_NAME")
    .sum("count")
    .withColumnRenamed("sum(count)", "total_flights_from")
    .sort(desc("total_flights_from"))
    .limit(5)
    .show()

  logger.info("Finished fundamentals")
  spark.close()
}

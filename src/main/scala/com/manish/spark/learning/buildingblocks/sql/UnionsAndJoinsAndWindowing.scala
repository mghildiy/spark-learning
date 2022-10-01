package com.manish.spark.learning.buildingblocks.sql

import com.manish.spark.learning.buildingblocks.sql.SparkSqlExample.spark
import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.expr

object UnionsAndJoinsAndWindowing extends App {

  val spark = Utils.createSparkSession("Unions, Joins And Windowing")

  Logger.getRootLogger().setLevel(Level.ERROR)

  val airports = "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\airport-nodes-na.txt"
  val departureDelays = "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\departuredelays.csv"

  val airportsDF = spark
    .read
    .option("header", "true")
    .option("inferschema", "true")
    .option("delimiter", "\t")
    .csv(airports)
  airportsDF.show(10)
  airportsDF.createOrReplaceTempView("airports_view")

  val departureDelaysDF =
    spark
      .read
      .option("header", "true")
      .csv(departureDelays)
      .withColumn("delay", expr("CAST(delay AS INT) AS delay"))
      .withColumn("distance", expr("CAST(distance AS INT) AS INT"))
  departureDelaysDF.show(10)
  departureDelaysDF.createOrReplaceTempView("departureDelays_view")

  val seattleToSF = departureDelaysDF
    .filter(expr("""origin == 'SEA' AND destination == 'SFO' AND
    date like '01010%' AND delay > 0"""))
  seattleToSF.createOrReplaceTempView("seattleToSF_view")
  seattleToSF.show()

  // union - same schema needed
  val unionDF = departureDelaysDF
    .union(seattleToSF)
  unionDF.createOrReplaceTempView("unionDF_view")
  unionDF
    .filter(
      """origin == 'SEA' AND destination == 'SFO' AND
        date like '01010%' AND delay > 0""")
    .show()
  spark
    .sql(
      """SELECT * from unionDF_view
        WHERE origin == 'SEA' AND destination == 'SFO'
        AND date like '01010%' AND delay > 0
        """)
    .show()

  // join
  /*seattleToSF
    .join(airportsDF.as("air"), $"air.IATA === $"origin")
    .show()*/
  /*seattleToSF.join(
    airportsDF,
    airportsDF.col("IATA") === $"origin"
  ).select("City", "State", "date", "delay", "distance", "destination").show()*/

  spark.sql("USE learn_spark_db")
  spark
    .sql("DROP TABLE IF EXISTS departureDelaysWindow")
  spark.sql(
    """CREATE TABLE departureDelaysWindow AS
      SELECT
      origin, destination, SUM(delay) AS totaldelays
      FROM departureDelays_view
      WHERE
      origin IN ('SFO', 'JFK', 'SEA')
      AND
      destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
      GROUP BY origin, destination
      """)
  spark
    .sql("""SELECT * FROM departureDelaysWindow""")
    .show()

}

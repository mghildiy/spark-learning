package com.manish.spark.learning.buildingblocks.sql

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}

object InbuiltHOFExample extends App {

  val spark = Utils.createSparkSession("HOF example")

  Logger.getRootLogger().setLevel(Level.ERROR)

  val t1 = Array(35, 36, 32, 30, 40, 42, 38)
  val t2 = Array(31, 32, 34, 55, 56)

  import spark.implicits._

  val celsiusDF = Seq(t1, t2).toDF("celsius")
  celsiusDF.createOrReplaceTempView("celsiusView")

  celsiusDF.show()

  transform.show()
  filter(38).show()
  exists.show()
  aggregate.show()

  spark.close()

  def transform = {
    spark
      .sql("SELECT celsius, transform(celsius, t -> ((t * 9) / 5) + 32) AS fahrenheit FROM celsiusView")
  }

  def filter(high: Int) = {
    spark
      .sql(s"SELECT celsius, filter(celsius, t -> t > $high) AS high FROM celsiusView")
  }

  def exists = {
    spark
      .sql("SELECT celsius, exists(celsius, t -> t = 38) AS threshold FROM celsiusView")
  }

  def aggregate = {
    spark.sql("SELECT" +
      " celsius," +
      " aggregate(celsius,0,(t, acc) -> t + acc,acc -> (acc div size(celsius) * 9 div 5) + 32 ) AS avgFahrenheit" +
      " FROM celsiusView")
  }
}

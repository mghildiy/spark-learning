package com.manish.spark.learning.buildingblocks

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

case class Country(name: String, capital: String)

object RowExample {

  def main(args: Array[String]) = {
    // Row object
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    println(blogRow(5))

    val sparkSession = SparkSession
      .builder()
      .appName("Rows Example")
      .config("spark.master", "local")
      .getOrCreate()

    // only show error logs
    Logger.getRootLogger().setLevel(Level.ERROR)

    // for toDF
    import sparkSession.implicits._

    // we can create dataframes in various possible ways apart from reading from file
    // from tuples
    val row1 = ("India", "New Delhi")
    val row2 = ("Japan", "Tokyo")
    Seq(row1, row2).toDF("Country", "Capital").show
    // from case class instances
    Seq(Country("Germany", "Berlin"), Country("France", "Paris")).toDF("Country", "Capital").show

    /*val usa = Row("USA", "Washington DC")
    val canada = Row("Canada", "Ottawa")
    Seq(usa, canada).toDF("Country", "Capital").show*/
  }

}

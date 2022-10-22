package com.manish.spark.learning.aggregation

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Aggregation extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Aggregation Demo")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val invoicesDF = spark
    .read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .load("src/main/resources/data/source/invoices.csv")
  //invoicesDF.show(5)
  //println(invoicesDF.schema)

  // column object expression on df
  val fromColExpr = invoicesDF
    .select(
      count("*").as("Total Number Of Invoices"),
      count("StockCode").as("Number of Unique Stockcodes"),
      sum("Quantity").as("Total Quantity"),
      avg("UnitPrice").as("Avg Unit Price"),
      //countDistinct("InvoiceNo").as("Number Of Unique Invoices")
    )

  // sql like expression on df
  val fromSqlExpr = invoicesDF
    .selectExpr(
      "count(1) as `Total Number Of Invoices`",
      "count(StockCode) as `Number of Unique Stockcodes`",
      "sum(Quantity) as `Total Quantity`",
      "avg(UnitPrice) as `Avg Unit Price`",
      //"distinct(InvoiceNo) as `Number Of Unique Invoices`"
    )

  //execute(fromSqlExpr)
  //execute(fromColExpr)

  // group by using pure sql way
  invoicesDF
    .createOrReplaceTempView("invoices")
  val groupByUsingPureSql = spark.sql(
    """
      | SELECT Country, InvoiceNo,
      | count(*) as Number,
      | sum(Quantity) as `Total Quantity`,
      | sum(Quantity * UnitPrice) as TotalInvoiceAmount
      | FROM invoices
      | GROUP BY Country, InvoiceNo
      |""".stripMargin)
  //execute(groupByUsingPureSql)

  // group by sing data frame
  val groupByUsingDF = invoicesDF
    .groupBy(col("Country"), col("InvoiceNo"))
    .agg(
      count("*").as("Total Number Of Items"),
      sum("Quantity").as("Total Quantity"),
      sum(expr("Quantity * UnitPrice")).as("TotalInvoiceAmount"),
      expr("sum(Quantity * UnitPrice) as TotalInvoiceAmount")
    )
  //execute(groupByUsingDF)

  val grpByCountryAndYWeek = invoicesDF
    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
    .filter("year(InvoiceDate) == 2010")
    .withColumn("Week", weekofyear(col("InvoiceDate")))
    .groupBy("Country", "Week")
    .agg(
      countDistinct("InvoiceNo").as("Num Invoices"),
      sum("Quantity").as("Total Quantity"),
      sum(expr("Quantity * UnitPrice")).as("TotalInvoiceAmount"),

    )
  grpByCountryAndYWeek
    .coalesce(1)
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/sink/parquet")

  grpByCountryAndYWeek
    .sort("Country", "Week")
    .show()

  spark.stop

  def execute(df: DataFrame): Unit = {
    val startTime = System.nanoTime()
    df.show()
    println((System.nanoTime() - startTime) / 1000000000.0)
  }

}

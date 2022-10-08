package com.manish.spark.learning.readingwriting

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class ExecutionPlanTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll() =
    spark = SparkSession
      .builder()
      .appName("Testing ExecutionPlan")
      .master("local[3]")
      .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("countByCountry works as expected") {
    // prepare test data
    val testDF = spark
      .read
      .option("samplingRatio", 0.5)
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/test/resources/data/reading-data.csv")

    // invoke unit
    val response = ExecutionPlan.countByCountry(testDF)

    // validate
    val countryByCount = new mutable.HashMap[String, Long]
    response.collect.foreach(row => countryByCount.put(row.getString(0), row.getLong(1)))
    assert(countryByCount("United States") == 4, "Count must be 4 for United States")
    assert(countryByCount("Canada") == 2, "Count must be 2 for Canada")
    assert(countryByCount("United Kingdom") == 1, "Count must be 1 for United Kingdom")
  }

}

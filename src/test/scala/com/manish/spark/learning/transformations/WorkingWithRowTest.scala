package com.manish.spark.learning.transformations

import com.manish.spark.learning.transformations.WorkingWithRow.toDateDF
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.sql.Date

case class TestRow(ID: String, EventDate: Date)
class WorkingWithRowTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _
  @transient var dataFrame: DataFrame = _

  override def beforeAll() = {
    spark = SparkSession
      .builder()
      .appName("Testing WorkingWithRow")
      .master("local[3]")
      .getOrCreate()

    val mySchema = StructType(
      List(
        StructField("ID", StringType),
        StructField("EventDate", StringType))
    )
    val rows = List(Row("101", "04/05/2020"), Row("102", "4/5/2020"), Row("103", "4/05/2020"))
    val rdd = spark.sparkContext.parallelize(rows, 2)
    dataFrame = spark.createDataFrame(rdd, mySchema)
  }

  override def afterAll(): Unit = spark.stop()

  test("toDateDF creates correct data type") {
    // invoke the unit
    val rows = toDateDF(dataFrame, "EventDate", "M/d/y").collectAsList()

    // validate
    rows.forEach(row => {
      assert(row.get(1).isInstanceOf[Date], "Must be Date type")
    })
  }

  test("toDateDF creates correct data value") {
    val sparkTest = spark
    import sparkTest.implicits._

    // invoke the unit
    val rows = toDateDF(dataFrame, "EventDate", "M/d/y").as[TestRow] .collectAsList()

    // validate
    rows.forEach(row => {
      assert(row.EventDate.toString == "2020-04-05", "Wrong value")
    })
  }

}

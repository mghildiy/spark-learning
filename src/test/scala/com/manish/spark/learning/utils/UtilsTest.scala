package com.manish.spark.learning.utils

//import com.manish.spark.learning.datareading.ExecutionPlan.dataSource
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class UtilsTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll() =
    spark = SparkSession
      .builder()
      .appName("Testing Utils")
      .master("local[3]")
      .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("Data is loaded correctly from data file") {
    // test data
    val testDF = Utils.dataFrameFromSampleAndFile(spark, 0.5, filePath = "src/test/resources/data/reading-data.csv", fileFormat = CSV)

    // invoke unit
    val numRecords = testDF.count()

    // validate
    assert(numRecords == 9, "Number of records must be 9")
  }

}

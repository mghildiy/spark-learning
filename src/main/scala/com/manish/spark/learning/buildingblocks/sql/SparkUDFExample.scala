package com.manish.spark.learning.buildingblocks.sql

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}

object SparkUDFExample {

  def main(args: Array[String]) = {
    val spark = Utils.createSparkSession("UDF Example")

    Logger.getRootLogger().setLevel(Level.ERROR)

    val squared = (x: Long) => x * x

    spark.udf.register("squared", squared)

    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark
      .sql("SELECT id, squared(id) FROM udf_test")
      .show()

    spark.close()
  }

}

package com.manish.spark.learning.buildingblocks

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}

object DataFrameReaderExample extends SFSchema {

  def main(args: Array[String]) = {
    val spark = Utils.createSparkSession("DataFrameReader Example")

    // only show error logs
    Logger.getRootLogger().setLevel(Level.ERROR)

    // creating dataframe by sampling, no need to define schema
    //val sfFireSamplingBasedDF = Utils.dataFrameFromSampleAndFile(spark, 0.001, true, "sf_fire_Incidents.csv", "csv")

    // creating dataframe using custom schema
    val sfFireSchemaBasedDF = Utils.dataFrameFromSchemaAndFile(spark, sfFireIncidentsSchema, "sf_fire_Incidents.csv", "csv")
    println(sfFireSchemaBasedDF.schema)
    sfFireSchemaBasedDF.printSchema

    // writing a dataframe into a file
    //sfFireSchemaBasedDF.write.format("parquet").save("D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\sf_fire_Incidents_write")
    // ...saving as a parquet table
    //sfFireSchemaBasedDF.write.format("parquet").saveAsTable("sf_fire_Incidents_table")

  }

}

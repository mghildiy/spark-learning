package com.manish.spark.learning.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SparkSession}

// DataFrameReader = SparkSession.read
// DataFrame = DataFrameReader
// .format(arg)....csv,json, parquet(default), avro etc etc
// .option("key", "value")
// .schema(args)...DDL String or StructType
// .load(arg)...path to data source

// DataframeWriter = DataFrame.write
// DataframeWriter
// .format(arg)....csv,json, parquet(default), avro etc etc
// .option("key", "value")
// .save(arg)
// .saveAsTable(arg)

object Utils {

  def createSparkSession(appName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appName)
      .master("local[3]")
      //.config("spark.master", "local")
      .getOrCreate()

  def dataFrameFromSchemaAndFile(spark: SparkSession,schema: StructType, fileName: String, fileFormat: String) = {
    val dataFrameReader = spark.read.schema(schema)
    createDataFrameFromFile(dataFrameReader, fileName, fileFormat)
  }

  def dataFrameFromSampleAndFile(spark: SparkSession, samplingRatio: Double, header: Boolean, fileName: String, fileFormat: String) = {
    val dataFrameReader = spark
      .read
      .option("samplingRatio", samplingRatio)
      .option("header", header)

    createDataFrameFromFile(dataFrameReader, fileName, fileFormat)
  }

  /*def dataSetFromFileAndCaseClass[T](spark: SparkSession, fileName: String, encoder: Encoder[T], fileFormat: String) =
    fileFormat match {
      case "csv" => spark.read.csv(fileName).as[encoder]
    }*/

  def getFilePath(fileName: String): String =
    getClass.getClassLoader.getResource(fileName).getFile

  private def createDataFrameFromFile(dataFrameReader: DataFrameReader, fileName: String, fileFormat: String) =
    fileFormat match {
      case "csv" => dataFrameReader.csv(fileName/*getFilePath(fileName)*/)
      case "json" => dataFrameReader.json(fileName/*getFilePath(fileName)*/)
      case _ => throw new Exception("File format not supported")
    }

}

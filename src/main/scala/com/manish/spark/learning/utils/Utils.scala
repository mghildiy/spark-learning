package com.manish.spark.learning.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import java.util.Properties
import scala.io.Source

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

  def createSparkSession(appName: String): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", appName)
    sparkConf.set("spark.master", "local[3]")
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }

  private def getSparkConf(confFilePath: String) = {
    val sparkConf = new SparkConf()
    val properties = new Properties
    properties.load(Source.fromFile(confFilePath).bufferedReader())
    properties.forEach((k,v) => sparkConf.set(k.toString, v.toString))

    sparkConf
  }

  def createSparkSessionFromConf(confFilePath: String): SparkSession = {
    SparkSession
      .builder()
      .config(getSparkConf(confFilePath))
      .getOrCreate()
  }


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

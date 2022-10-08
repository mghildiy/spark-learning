package com.manish.spark.learning.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import java.util.Properties
import scala.io.Source

sealed trait FileFormat
object CSV extends FileFormat
object JSON extends FileFormat

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


  def dataFrameFromSchemaAndFile(spark: SparkSession,schema: StructType, filePath: String, fileFormat: FileFormat) = {
    val dataFrameReader = spark.read.schema(schema)
    createDataFrameFromFile(dataFrameReader, filePath, fileFormat)
  }

  def dataFrameFromSampleAndFile(spark: SparkSession, samplingRatio: Double, header: Boolean = true, inferSchema: Boolean = true, filePath: String, fileFormat: FileFormat) = {
    val dataFrameReader = spark
      .read
      .option("samplingRatio", samplingRatio)
      .option("header", header)
      .option("inferSchema", inferSchema)

    createDataFrameFromFile(dataFrameReader, filePath, fileFormat)
  }

  /*def dataSetFromFileAndCaseClass[T](spark: SparkSession, fileName: String, encoder: Encoder[T], fileFormat: String) =
    fileFormat match {
      case "csv" => spark.read.csv(fileName).as[encoder]
    }*/

  def getFilePath(fileName: String): String =
    getClass.getClassLoader.getResource(fileName).getFile

  private def createDataFrameFromFile(dataFrameReader: DataFrameReader, filePath: String, fileFormat: FileFormat) =
    fileFormat match {
      case CSV => dataFrameReader.csv(filePath)
      case JSON => dataFrameReader.json(filePath)
      case _ => throw new Exception("File format not supported")
    }

}

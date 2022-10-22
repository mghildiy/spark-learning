package com.manish.spark.learning.transformations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)
object UnstructuredData extends App with Serializable {

  @transient val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder()
    .appName("Unstructured data Demo")
    .config("spark.master", "local[3]")
    .getOrCreate()

  val stringDataSet = spark.read
    .textFile("src/main/resources/data/source/apache_logs.txt")
  println(stringDataSet.schema)
  val logsDF = stringDataSet.toDF()
  println(logsDF.schema)

  val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

  import spark.implicits._

  val logDS = logsDF.map(row =>
    row.getString(0) match {
      case myReg(ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) =>
        ApacheLogRecord(ip, date, request, referrer)
    }
  )
  println(logDS.schema)
  val logDF = logDS.toDF()
  println(logDF.schema)

  val a = logDS
    .filter(row => row.referrer.trim != "-")
  val b = logDF
    .filter(row => row.getString(3).trim != "-")
  val c = logDS
    .where("trim(referrer) != '-'")
  val d = logDF
    .where("trim(referrer) != '-'")

  import org.apache.spark.sql.functions._
  logDF
    .where("trim(referrer) != '-' ")
    .withColumn("referrer", substring_index($"referrer", "/", 3))
    .groupBy("referrer").count().show(truncate = false)


  spark.stop()

}

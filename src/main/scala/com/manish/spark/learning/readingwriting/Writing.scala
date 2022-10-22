package com.manish.spark.learning.readingwriting

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.spark_partition_id

object Writing extends App {

  val logger = Logger.getLogger(getClass.getName)

  if(args.length < 3) {
    logger.info("Data source file or conf file or sink not provided")
    System.exit(1)
  }

  val dataSource = args(0)
  val confFile = args(1)
  val sink = args(2)
  logger.info(s"Data source file:$dataSource")
  logger.info(s"Conf file:$confFile")
  logger.info(s"Data Sink:$sink")
  logger.info("Creating spark session")
  val spark = Utils.createSparkSessionFromConf(confFile)

  val dataFrame = spark
    .read
    .format("parquet")
    .option("path", dataSource)
    .load()
  //dataFrame.show(5)
  logger.info(s"Number of partitions:${dataFrame.rdd.getNumPartitions}")
  dataFrame.groupBy(spark_partition_id()).count().show()
  val repartitionedDataframe = dataFrame.repartition(5)
  logger.info(s"Number of partitions after repartition:${repartitionedDataframe.rdd.getNumPartitions}")
  repartitionedDataframe.groupBy(spark_partition_id()).count().show()

  /*repartitionedDataframe.write
    .format("avro")
    .mode(SaveMode.Overwrite)
    .option("path", sink)
    .save()*/

  /*dataFrame.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/sink/json")
    .partitionBy("OP_CARRIER", "ORIGIN")
    .option("maxRecordsPerFile", 10000)
    .save()*/

  spark.sql("CREATE DATABASE IF NOT EXISTS flight_time_db")
  spark.catalog.setCurrentDatabase("flight_time_db")

  dataFrame.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(5, "OP_CARRIER", "ORIGIN")
    .sortBy("OP_CARRIER", "ORIGIN")
    .saveAsTable("flight_time_tbl")

  spark.catalog.listTables("flight_time_db").show()

  logger.info("Writing to the sink finished")
  spark.close()
}

package com.manish.spark.learning.api

import com.manish.spark.learning.utils.{CSV, Utils}
import org.apache.log4j.Logger

case class Record( Age: Int, Gender: String, Country: String, state: String)
object DataSet extends App {

  val logger = Logger.getLogger(getClass.getName)

  if(args.length < 2) {
    logger.info("Data source file or conf file not provided")
    System.exit(1)
  }

  val dataSource = args(0)
  val confFile = args(1)
  logger.info(s"Data source file:$dataSource")
  logger.info(s"Conf file:$confFile")
  logger.info("Creating spark session")
  val spark = Utils.createSparkSessionFromConf(confFile)

  val dataFrame = Utils
    .dataFrameFromSampleAndFile(spark, 0.5, true, true, filePath = dataSource, fileFormat = CSV)
    .select("Age","Gender","Country","state")
  import spark.implicits._
  val ds = dataFrame.as[Record]
  ds.groupByKey(r => r.Age < 40)

}

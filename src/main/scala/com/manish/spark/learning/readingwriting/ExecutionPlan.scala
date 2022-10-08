package com.manish.spark.learning.readingwriting

import com.manish.spark.learning.utils.{CSV, Utils}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object ExecutionPlan extends App {

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
  val dataFrameWith2Partitions = dataFrame.repartition(2)
  val countByCountryDataFrame = countByCountry(dataFrameWith2Partitions)
  logger.info(countByCountryDataFrame.collect().mkString("->"))

  logger.info("Execution plan finished")
  //scala.io.StdIn.readLine()
  spark.close()

  def countByCountry(dataFrame: DataFrame) = {
    dataFrame
      .where("Age < 40")
      .select("Age","Gender","Country","state")
      .groupBy("Country")
      .count()
  }

}


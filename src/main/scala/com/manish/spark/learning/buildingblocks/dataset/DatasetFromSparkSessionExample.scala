package com.manish.spark.learning.buildingblocks.dataset

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.desc

import scala.util.Random

case class Usage(uid: Int, uname: String, usage: Int)

object DatasetFromSparkSessionExample extends App {

  val spark = Utils.createSparkSession("Dataset from Spark session")
  import spark.implicits._

  Logger.getRootLogger().setLevel(Level.ERROR)

  val r = new Random()
  val data = for {
    i <- 1 to 100
  } yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))

  val dsUsage = spark.createDataset(data)
  dsUsage.show()

  dsUsage
    .filter(d => d.usage> 900)
    .orderBy(desc("usage"))
    .show(5, false)

  spark.stop()

}

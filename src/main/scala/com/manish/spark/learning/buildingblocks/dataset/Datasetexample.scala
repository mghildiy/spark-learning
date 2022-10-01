package com.manish.spark.learning.buildingblocks.dataset

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}

case class Blogger(id:Long, first:String, last:String, url:String, published:String,
                   hits: Long, campaigns:Array[String])
object Datasetexample extends App {

    val spark = Utils.createSparkSession("Dataset")

    Logger.getRootLogger().setLevel(Level.ERROR)

    import spark.implicits._

    val bloggers = "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("path", bloggers)
      .load() // dataframe
      .as[Blogger] // to dataset
    bloggersDS.show()

    spark.stop()

}

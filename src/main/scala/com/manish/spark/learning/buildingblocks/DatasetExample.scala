package com.manish.spark.learning.buildingblocks

import com.manish.spark.learning.utils.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset

case class DeviceIoTData (battery_level: Long, c02_level: Long,
                          cca2: String, cca3: String, cn: String, device_id: Long,
                          device_name: String, humidity: Long, ip: String, latitude: Double,
                          lcd: String, longitude: Double, scale:String, temp: Long,
                          timestamp: Long)
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
                               cca3: String)

case class Blogger(id:Long, first:String, last:String, url:String, published:String,
                   hits: Long, campaigns:Array[String])

object DatasetExample extends SFSchema {

  def main(args: Array[String]) = {
    val spark = Utils.createSparkSession("Dataset exploration")

    Logger.getRootLogger().setLevel(Level.ERROR)

    import spark.implicits._
    val ds = spark
      .read
      .json("D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\iot_devices.json")
      .as[DeviceIoTData]
    ds.printSchema()
    ds.show(4, false)
    filter(ds).show(5, false)
    // we can use scala/java's native expressions
    ds
      .filter(d => d.temp > 25)
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]
      .show()
    // ...or, we can use SQL-like API like with DataFrame
    ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where($"temp" > 25)
      .as[DeviceTempByCountry]
      .show(4, false)


    spark.stop()
  }

  private def filter(dataSet: Dataset[DeviceIoTData]) =
    dataSet
      .filter(d => d.temp > 30 && d.humidity > 70)

}

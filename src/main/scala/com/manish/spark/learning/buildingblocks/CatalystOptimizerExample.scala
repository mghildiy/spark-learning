package com.manish.spark.learning.buildingblocks

import com.manish.spark.learning.utils.{CSV, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col

// https://blog.clairvoyantsoft.com/spark-logical-and-physical-plans-469a0c061d9e
object CatalystOptimizerExample extends SFSchema {
  val spark = Utils.createSparkSession("Catalyst optimizer exploration")
  val sfFireSchemaBasedDF = Utils.dataFrameFromSchemaAndFile(spark
    , sfFireIncidentsSchema, "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\sf_fire_Incidents.csv", CSV)

  def main(args: Array[String]) = {

    Logger.getRootLogger().setLevel(Level.ERROR)

    val incidentsWithPriority = sfFireSchemaBasedDF
      .select("IncidentNumber", "OriginalPriority")
      .where(col("OriginalPriority") =!= "A")
      .where(col("OriginalPriority") =!= "B")

    // Unresolved plan -> Analyzed plan -> Optimized plan -> Physical plan
    println("###############Parsed/unresolved logical plan###############")
    println(incidentsWithPriority.queryExecution.logical)
    println("###############Analyzed/resolved logical plan###############")
    println(incidentsWithPriority.queryExecution.analyzed)
    println("###############Optimized logical plan###############")
    println(incidentsWithPriority.queryExecution.optimizedPlan)
  }

}

package com.manish.spark.learning.buildingblocks

import com.manish.spark.learning.utils.{CSV, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, desc, to_date, to_timestamp}

object OperationsOnDataFrame extends SFSchema {

  val spark = Utils.createSparkSession("DataFrame exploration")
  val sfFireSchemaBasedDF = Utils.dataFrameFromSchemaAndFile(spark
    , sfFireIncidentsSchema, "D:\\work\\learning\\dataengg\\spark\\spark-learning\\src\\main\\resources\\sf_fire_Incidents.csv", CSV)
    sfFireSchemaBasedDF.printSchema()

  def main(args: Array[String]) = {
    Logger.getRootLogger().setLevel(Level.ERROR)

    projectAsSQLSelect
    filterAsSQLWhere
    countDistinctValues
    listDistinctValues
    renamingColumn
    addingAndDroppingColumn
      .select("Date of Incident")
      .distinct()
      .orderBy("Date of Incident")
      .show()
    groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    spark.close()
  }

  private def groupBy(colName: String, cols: String*) =
    sfFireSchemaBasedDF
      .select(colName, cols:_*)
      .where(col(colName).isNotNull)
      .groupBy(colName, cols:_*)

  private def projectAsSQLSelect =
    sfFireSchemaBasedDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")

  private def filterAsSQLWhere =
    sfFireSchemaBasedDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

  private def countDistinctValues() = {
    import org.apache.spark.sql.functions._
    sfFireSchemaBasedDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(countDistinct("CallType") as "DistinctCallTypes")
  }

  private def listDistinctValues =
    sfFireSchemaBasedDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct()

  private def renamingColumn =
    sfFireSchemaBasedDF
      .withColumnRenamed("Delay", "ResponseDelayedinMins")
      .select("ResponseDelayedinMins")
      .where(col("ResponseDelayedinMins") > 5)
      // .where(expr("ResponseDelayedinMins > 5"))

  private def addingAndDroppingColumn =
    sfFireSchemaBasedDF
      .withColumn("Date of Incident", to_date(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

}

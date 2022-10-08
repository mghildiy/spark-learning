package com.manish.spark.learning

import com.manish.spark.learning.utils.{JSON, Utils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, expr}
import org.apache.spark.sql.types._

object SchemaCreation {

  def main(args: Array[String]) = {
    // defining schema programmatically
    val pSchema = StructType(
      Array(
        StructField("author", StringType, false),
        StructField("title", StringType, false),
        StructField("pages", IntegerType, false)
      )
    )

    // defining schema using DDL
    val ddlSchema = "author STRING, title STRING, pages INT"

    // creating Dataframe, reading data from a file
    val sparkSession = SparkSession
                          .builder()
                          .appName("DDL Schema Creation")
                          .config("spark.master", "local")
                          .getOrCreate()

    // only show error logs
    Logger.getRootLogger().setLevel(Level.ERROR)

    // creating dataframe with a schema and data from a file
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val blogsDF = Utils.dataFrameFromSchemaAndFile(sparkSession, schema, "blogs.json", JSON)
    blogsDF.show(false)
    println(blogsDF.printSchema)
    println(blogsDF.schema)

    println(blogsDF.columns.mkString(","))
    println(blogsDF.col("Url"))
    // a new dataframe with column 'Hits * 2'
    blogsDF.select(col("Hits") * 2).show()
    blogsDF.select(expr("Hits * 2")).show()
    // a new dataframe with an additional column
    blogsDF.withColumn("Huge Hitters", expr("Hits > 10000")).show()
    // concatenate columns
    blogsDF.withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
      .select(col("AuthorsId"))
      .show()
    // sort by a column
    blogsDF.sort(col("Id").desc).show()
    //blogsDF.sort($"Id".desc).show()

    sparkSession.close()
  }

}

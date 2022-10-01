/*ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-learning"
  )*/

// Name of the package
name := "spark-learning"
// Version of our package
version := "1.0"
// Version of Scala
scalaVersion := "2.12.14"
val sparkVersion = "3.3.0"
// Spark library dependencies
useCoursier := false
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies

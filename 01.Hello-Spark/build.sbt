name := "01.Hello-Spark"
organization := "com.javt"
version := "0.1"

scalaVersion := "2.12.18"
autoScalaLibrary := false

val sparkVersion = "3.5.0"
val log4jVersion = "2.20.0"

val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    // logging
    "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
    "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
)

val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % "3.2.18" % Test
)

libraryDependencies := sparkDependencies ++ testDependencies

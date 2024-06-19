name := "6.Data Sink Demo"
organization := "com.javt"
version := "0.1"

scalaVersion := "2.12.18"
autoScalaLibrary := false

val sparkVersion = "3.5.0"

val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-avro" % sparkVersion
)

libraryDependencies := sparkDependencies 

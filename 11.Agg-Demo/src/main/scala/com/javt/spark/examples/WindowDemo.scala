package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .appName("Aggregation Demo")
     .master("local[3]")
     .enableHiveSupport()
     .getOrCreate()

  val summaryDF = spark.read.parquet("output/part*.parquet")

   val runningTotalWindow = Window.partitionBy("Country")
     .orderBy("WeekNumber")
     .rowsBetween(Window.unboundedPreceding, Window.currentRow)

   summaryDF.withColumn("RunningTotal",
     sum("InvoiceValue").over(runningTotalWindow)
   ).show()

   spark.stop()

 }



}
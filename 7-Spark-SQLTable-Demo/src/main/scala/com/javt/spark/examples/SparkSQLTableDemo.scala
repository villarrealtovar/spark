package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}


object SparkSQLTableDemo extends Serializable {

 def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

   val spark = SparkSession.builder()
     .appName("Spark SQL Table Demo")
     .master("local[3]")
     .enableHiveSupport()
     .getOrCreate()

   val flightTimeParquetDF = spark.read
     .format("parquet")
     .option("path", "dataSources")
     .load()

   spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
   spark.catalog.setCurrentDatabase("AIRLINE_DB")

   flightTimeParquetDF.write
     .format("csv")
     .mode(SaveMode.Overwrite)
     // .partitionBy("ORIGIN", "OP_CARRIER")
     .bucketBy(5, "ORIGIN", "OP_CARRIER")
     .sortBy("ORIGIN", "OP_CARRIER")
     .saveAsTable("flight_data_tbl")

   spark.catalog.listTables("AIRLINE_DB").show()
   logger.info("Finished")

   spark.stop()

 }   
}
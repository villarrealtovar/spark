package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloSparkSQL extends Serializable {

 def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
        logger.info("Usage: HelloSparkSQL Filename not found")
        System.exit(1)
    }

   val spark = SparkSession.builder()
     .appName("Hello SparkSQLf")
     .master("local[3]")
     .getOrCreate()

   val surveyDF = spark.read
     .option("header", "true")
     .option("inferSchema", "true")
     .csv(args(0))

   surveyDF.createOrReplaceTempView("survey_tbl")

   val countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age < 40 group by Country")

   logger.info(countDF.collect().mkString(","))

   spark.stop()

 }   
}
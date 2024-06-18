package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloDataset extends Serializable {

 def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
        logger.info("Usage: HelloDataset Filename not found")
        System.exit(1)
    }

   val spark = SparkSession.builder()
     .appName("Hello Dataset")
     .master("local[3]")
     .getOrCreate()

   val rawDF: Dataset[Row] = spark.read
     .option("header", "true")
     .option("inferSchema", "true")
     .csv(args(0))

   import spark.implicits._
   val surveyDS: Dataset[SurveyRecord] = rawDF.select("Age", "Gender", "Country", "state").as[SurveyRecord]
   val filteredDS = surveyDS.filter(_.Age < 40)
   val filteredDF = surveyDS.filter("Age < 40")

   // Type safe GroupBy
   val countDS = filteredDS.groupByKey(_.Country).count
   // Runtime GroupBy
   val countDF = filteredDS.groupBy("Country").count

   logger.info("Dataframe:" + countDF.collect().mkString(","))
   logger.info("Dataset:" + countDS.collect().mkString(","))

   spark.stop()

 }   
}
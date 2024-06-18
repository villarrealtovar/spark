package com.javt.spark.examples

import _root_.org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

object HelloSpark extends Serializable {

    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    

    def main(args: Array[String]): Unit = {
      if (args.length == 0) {
        logger.error("Usage: HelloSpark filename no declared as argument")
        System.exit(1)
      }

      logger.info("Starting Hello Spark")

      val spark = SparkSession.builder()
        .config(getSparkAppConf)
        .getOrCreate()

      val surveyRawDF = loadSurveyDF(spark, args(0))
      val partitionedSurveyDF = surveyRawDF.repartition(2)
      val countDF = countByCountry(partitionedSurveyDF)
      logger.info(countDF.collect().mkString("->"))

      // Process your data

      logger.info("Finished Hello Spark")
      // scala.io.StdIn.readLine()
      spark.close()
    }

    def countByCountry(surveyDF: DataFrame): DataFrame = {
      surveyDF
        .where("Age < 40")
        .select("Age", "Gender", "Country", "state")
        .groupBy("Country")
        .count()
    }

    def loadSurveyDF(spark: SparkSession, dataFile: String): DataFrame = {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(dataFile)
    }

    def getSparkAppConf: SparkConf = {
      val sparkAppConf = new SparkConf()
      val props = new Properties()
      props.load(Source.fromFile("spark.conf").bufferedReader())
      props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))

      // This is a fix for Scala 2.11
      // import scala.collection.JavaConverters._
      // props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
      sparkAppConf
    }


    

}

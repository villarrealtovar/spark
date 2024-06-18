package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class SurveyRecord(Age: Int, Gender: String, Country: String, state: String)

object HelloRDD extends Serializable {

 def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

    if (args.length == 0) {
        logger.info("Usage: HelloRDD Filename not found")
        System.exit(1)
    }

   // Create a Spark Context
   val sparkAppConf = new SparkConf().setAppName("HelloRDD").setMaster("local[3]")
   val sparkContext = new SparkContext(sparkAppConf)

   val linesRDD = sparkContext.textFile(args(0))
   val partitionedRDD = linesRDD.repartition(2)
   val colsRDD = partitionedRDD.map(line => line.split(",").map(_.trim))
   val selectRDD = colsRDD.map(col => SurveyRecord(col(1).toInt, col(2), col(3), col(4) ))
   val filterRDD = selectRDD.filter(row => row.Age < 40)
   val kvRDD = filterRDD.map(row => (row.Country, 1))
   val countRDD = kvRDD.reduceByKey((v1, v2) => v1 + v2)

   logger.info(countRDD.collect().mkString(","))

   sparkContext.stop()
 }   
}
package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.spark_partition_id

object DataSinkDemo extends Serializable {

 def main(args: Array[String]): Unit = {
    @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

   val spark = SparkSession.builder()
     .appName("Hello SparkSQLf")
     .master("local[3]")
     .getOrCreate()

   val flightTimeParquetDF = spark.read
     .format("parquet")
     .option("path", "dataSources")
     .load()

   logger.info("Num Partitions before: " + flightTimeParquetDF.rdd.getNumPartitions)
   flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

   val partitionedDF = flightTimeParquetDF.repartition(5)

   logger.info("Num Partitions after: " + partitionedDF.rdd.getNumPartitions)
   partitionedDF.groupBy(spark_partition_id()).count().show()

   /*
   partitionedDF.write
     .format("avro")
     .mode(SaveMode.Overwrite)
     .option("path", "dataSink/avro/")
     .save()
*/


   flightTimeParquetDF.write
     .format("json")
     .mode(SaveMode.Overwrite)
     .option("path", "dataSink/json/")
     .partitionBy("OP_CARRIER", "ORIGIN")
     .option("maxRecordsPerFile", 10000 )
     .save()

   logger.info("Finished Partitioning")

   spark.stop()

 }   
}
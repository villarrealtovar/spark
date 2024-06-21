package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .appName("Aggregation Demo")
     .master("local[3]")
     .enableHiveSupport()
     .getOrCreate()

   val invoiceDF = spark.read
     .format("csv")
     .option("header", "true")
     .option("inferSchema", "true")
     .load("data/invoices.csv")

   // simple aggregation
   invoiceDF.select(
     count("*").as("Count *"),
     sum("Quantity").as("TotalQuantity"),
     avg("UnitPrice").as("AvgPrice"),
     countDistinct("InvoiceNo").as("CountDistinct")
   ).show()

   invoiceDF.selectExpr(
     "count(1) as `count 1`",
     "count(StockCode) as `count field`",
     "sum(Quantity) as TotalQuantity",
     "avg(UnitPrice) as AvgPrice"
   ).show()


   // Grouping
   invoiceDF.createTempView("sales")
   val summarySQLDF = spark.sql(
     """
       |SELECT Country, InvoiceNo,
       |sum(Quantity) as TotalQuantity,
       |round(sum(Quantity*UnitPrice), 2) as InvoiceValue
       |FROM sales
       |GROUP BY Country, InvoiceNo
       |""".stripMargin
   )

   summarySQLDF.show()

   val summaryDF = invoiceDF.groupBy("Country", "InvoiceNo")
     .agg(
       sum("Quantity").as("TotalQuantity"),
       round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue"),
       expr("round(sum(Quantity*UnitPrice), 2) as InvoiceValue")
     )

   summaryDF.show()

   // exercise
   val NumInvoices = countDistinct("InvoiceNo").as("NumInvoices")
   val TotalQuantity = sum("Quantity").as("TotalQuantity")
   val InvoiceValue = round(sum(expr("Quantity*UnitPrice")), 2).as("InvoiceValue")


   val exerciseDF = invoiceDF
     .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
     .where("year(InvoiceDate) == 2010")
     .withColumn("WeekNumber", weekofyear(col("InvoiceDate")))
     .groupBy("Country", "WeekNumber")
     .agg(
       NumInvoices,
       TotalQuantity,
       InvoiceValue
     )

   exerciseDF.coalesce(1)
     .write
     .format("parquet")
     .mode("overwrite")
     .save("output")

   exerciseDF.sort("Country", "WeekNumber").show()

   spark.stop()

 }



}
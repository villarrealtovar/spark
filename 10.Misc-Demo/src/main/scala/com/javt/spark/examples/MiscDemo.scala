package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.IntegerType

object MiscDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

 def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
     .appName("Misc Demo")
     .master("local[3]")
     .enableHiveSupport()
     .getOrCreate()


   val dataList = List(
     ("Andres", "11", "08", "82"), // 1981
     ("Caro", "08", "03", "1984"),
     ("Thiago", "18", "04", "18"), // 2006
     ("Jose", "02", "02", "59"), // 1963
     ("Lucia", "25", "08", "61"), // 1961
     ("Caro", "08", "03", "1984")
   )

   val rawDF = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(3)
   rawDF.printSchema()

   import org.apache.spark.sql.functions.monotonically_increasing_id
   import org.apache.spark.sql.functions.expr
   import org.apache.spark.sql.functions.col
   val finalDF1 = rawDF.withColumn("id", monotonically_increasing_id)
     .withColumn("year", expr(
       """
         |case when year < 21 then cast(year as int) + 2000
         |when year < 100 then cast(year as int) + 1900
         |else year
         |end
         |""".stripMargin))

   finalDF1.show()

   val finalDF2 = rawDF.withColumn("id", monotonically_increasing_id)
     .withColumn("day", col("day").cast(IntegerType))
     .withColumn("month", col("month").cast(IntegerType))
     .withColumn("year", col("year").cast(IntegerType))
     .withColumn("year", expr(
       """
         |case when year < 21 then year + 2000
         |when year < 100 then year + 1900
         |else year
         |end
         |""".stripMargin))

    finalDF2.show()

   import org.apache.spark.sql.functions.when
   import org.apache.spark.sql.functions.to_date
   val finalDF3 = rawDF.withColumn("id", monotonically_increasing_id)
     .withColumn("day", col("day").cast(IntegerType))
     .withColumn("month", col("month").cast(IntegerType))
     .withColumn("year", col("year").cast(IntegerType))
     .withColumn("year",
       when(col("year") < 21, col("year") + 2000)
       when(col("year") < 100, col("year") + 1900)
       otherwise(col("year"))
     )
     .withColumn("dobString", expr("concat(day, '/', month, '/', year)"))
     .withColumn("dob1", expr("to_date(concat(day, '/', month, '/', year), 'd/M/y')")) // using to_date in sql expression
     .withColumn("dob2", to_date(expr("concat(day,'/',month,'/',year)"), "d/M/y")) // using to_date as object
     .drop("day", "month", "year")
     .dropDuplicates("name", "dob1")
     .sort(expr("dob1").desc)

   finalDF3.show()


   spark.stop()

 }



}
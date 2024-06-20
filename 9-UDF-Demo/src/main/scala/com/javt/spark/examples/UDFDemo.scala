package com.javt.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object UDFDemo extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

 def main(args: Array[String]): Unit = {

   if (args.length == 0 ) {
     logger.error("Usage: UDFDemo filename needed.")
     System.exit(1)
   }

   val spark = SparkSession.builder()
     .appName("Spark SQL Table Demo")
     .master("local[3]")
     .enableHiveSupport()
     .getOrCreate()

  val surveyDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(args(0))

   surveyDF.show(10, truncate = false)

   import org.apache.spark.sql.functions._

   // register an UDF as column object
   spark.catalog.listFunctions().filter(_.name == "parseGenderUDF").show() // udf() function doesn't register the parseGenderUDF in catalog database
   val parseGenderUDF = udf(parseGender(_:String): String)
   val surveyDF2 = surveyDF.withColumn("Gender", parseGenderUDF(col("Gender")))
   surveyDF2.show(10, truncate = false)

   // register an UDF as SQL expression

   spark.udf.register("parseGenderUDF", parseGender(_: String): String) // udf.register register function in catalog database
   spark.catalog.listFunctions().filter(_.name == "parseGenderUDF").show()
   val surveyDF3 = surveyDF.withColumn("Gender", expr("parseGenderUDF('Gender')") )


   spark.stop()
 }

  def parseGender(s: String): String = {
    val femaleRegPattern = "^f$|f.m|w.m".r
    val maleRegPattern = "^m$|ma|m.1".r

    if (femaleRegPattern.findFirstIn(s.toLowerCase).nonEmpty) "Fenale"
    else if (maleRegPattern.findFirstIn(s.toLowerCase).nonEmpty) "Male"
    else "Unknown"

  }


}
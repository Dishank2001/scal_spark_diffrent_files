package com.example.md1
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object test2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("JsonStreamProcessor")
      .master("local[*]")
      .getOrCreate()

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val jsonStreamDF = spark.readStream
      .option("maxFilesPerTrigger", 1) // Process one file per trigger
      .option("inferSchema", "true")
      .json("C:\\Users\\User\\Desktop\\test")

    val parquetOutputQuery = jsonStreamDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .start()

    parquetOutputQuery.awaitTermination()


  }

}

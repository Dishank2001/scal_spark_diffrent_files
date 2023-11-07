package com.example.md1


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object test1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("JsonStreamProcessor")
      .master("local[*]")
      .getOrCreate()

    // Enable schema inference for streaming queries
    spark.sql("set spark.sql.streaming.schemaInference=true")

    val jsonStreamDF = spark.readStream
      .option("maxFilesPerTrigger", 1) // Process one file per trigger
      .json("C:\\Users\\User\\Desktop\\scala")

    val kafkaOutputQuery = jsonStreamDF
      .selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.109:9092")
      .option("topic", "test3")
      .option("checkpointLocation", "C:\\Users\\User\\Desktop\\scala")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    kafkaOutputQuery.awaitTermination()
  }
}

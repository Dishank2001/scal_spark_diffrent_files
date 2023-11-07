package com.example.md1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object kafka_to_console_streaming_data {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("KafkaToParquet")
      .master("local[*]")
      .getOrCreate()

    val kafkaBootstrapServers = "192.168.0.109:9092"
    val kafkaTopic = "test2"
    val outputDirectory = "D:\\output"

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets","earliest")
      .load()

    val jsonStreamDF = kafkaStreamDF.selectExpr("CAST(value AS STRING)")


    // Write processed data to Parquet format
    val parquetOutputQuery = jsonStreamDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.AvailableNow())
      .start()

    parquetOutputQuery.awaitTermination()
  }
}
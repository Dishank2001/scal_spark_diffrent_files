package com.example.md1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object kafka_to_csv_consumer_spark_scala_using_read {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("KafkaSparkIntegration")
      .master("local[*]")
      .getOrCreate()

    // Define Kafka broker and topic
    val kafkaBrokers = "192.168.0.109:9092" // Replace with your Kafka broker(s)
    val kafkaTopic = "test1" // Replace with your Kafka topic

    // Create a DataFrame to read data from Kafka
    val kafkaDataFrame = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .load()

    // Process the Kafka data (e.g., convert key and value to strings, add a timestamp column)
    val processedDataFrame = kafkaDataFrame
      .selectExpr( "CAST(value AS STRING)")
      .withColumn("timestamp", current_timestamp())


    // Show the processed data (you can perform further operations or write data to other systems)
    processedDataFrame.show()

    // Stop the SparkSession when done
    spark.stop()
  }
}

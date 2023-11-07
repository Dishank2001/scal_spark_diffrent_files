package com.example.md1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._

object csv_to_kafka_producer_spark_scala_using_write {
  def main(args: Array[String]): Unit = {
    // Create a SparkConf and SparkSession
    val conf = new SparkConf()
      .setAppName("ScalaSparkExample")
      .setMaster("local[*]") // Use local mode for testing; change as needed
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // Read a CSV file into a DataFrame
    val inputPath = "C:\\Users\\User\\Desktop\\open_ai_resume_parser\\Resume_data.csv"
    val df: DataFrame = spark.read
      .option("header", "true") // Use the first row as headers
      .option("inferSchema", "true") // Infer column data types
      .csv(inputPath)

    // Show the schema of the DataFrame
    df.printSchema()
    df.show()

    val kafkaBootstrapServers = "192.168.0.109:9092"

    val kafkaProducerProps: Map[String, String] = Map(
      "bootstrap.servers" -> kafkaBootstrapServers,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )


    df
        .selectExpr("CAST(Phone AS STRING) as key", "to_json(struct(*)) as value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrapServers)
        .option("topic", "test1")
        .option("checkpointLocation", "C:\\Users\\User\\Downloads")
        .options(kafkaProducerProps)
        .save()

    // Perform a simple transformation: Count the occurrences of each value in a column
    val valueCounts = df.groupBy("name").count()

    // Show the transformed DataFrame
    valueCounts.show()

    // Stop the Spark session when done
//    spark.stop()
  }
}

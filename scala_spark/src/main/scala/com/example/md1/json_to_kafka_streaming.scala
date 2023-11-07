package com.example.md1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object json_to_kafka_streaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("JsonStreamProcessor")
      .master("local[*]")
      .getOrCreate()
    val schema: StructType = StructType(Seq(
      StructField("visitor_id", StringType),
      StructField("average_time_spent", LongType),
      StructField("mode_of_payment", StringType),
      StructField("total_amount", IntegerType),
      StructField("item_type", StringType)
    ))

    val jsonStreamDF = spark.readStream
      .option("maxFilesPerTrigger", 1) // Process one file per trigger
      .schema(schema)
      .json("C:\\Users\\User\\Desktop\\scala")

    val kafkaOutputQuery = jsonStreamDF
      .selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.109:9092")
      .option("topic", "test2")
      .option("checkpointLocation", "C:\\Users\\User\\Desktop\\scala")
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start()

    kafkaOutputQuery.awaitTermination()
  }
}

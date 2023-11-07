//package com.example.md1
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.streaming.OutputMode
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import java.util.Properties
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
//object test {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder
//      .appName("CSVToKafkaStructuredStreaming")
//      .getOrCreate()
//
//    val kafkaBrokers = "192.168.0.109:9092" // Change to your Kafka broker address
//    val kafkaTopic = "test1" // Kafka topic to write to
//    val csvDirectory = "C:\\Users\\User\\Desktop\\open_ai_resume_parser\\Resume_data.csv" // Replace with the path to your CSV files
//
//    val schema = StructType(
//      Array(
//        StructField("Name", StringType, true),
//        StructField("Email", StringType, true),
//        StructField("Phone", StringType, true),
//        StructField("University", StringType, true),
//        StructField("Experience (in year only)", StringType, true)
//      )
//    )
//
//    val csvStream = spark
//      .readStream
//      .option("header", "true")
//      .schema(schema)
//      .csv(csvDirectory)
//
//    val query = csvStream
//      .selectExpr(
//        "Name as Name",
//        "Email as Email",
//        "Phone as Phone",
//        "University as University",
//        "`Experience (in year only)` as Experience"
//      )
//      .writeStream
//      .outputMode(OutputMode.Append())
//      .foreach { row =>
//        // Use a Kafka producer to write the data to Kafka
//        val kafkaProducer = createKafkaProducer(kafkaBrokers)
//        val name = row.getAs[String]("Name")
//        val email = row.getAs[String]("Email")
//        val phone = row.getAs[String]("Phone")
//        val university = row.getAs[String]("University")
//        val experience = row.getAs[String]("Experience")
//        val data = s"$name,$email,$phone,$university,$experience"
//        val record = new ProducerRecord[String, String](kafkaTopic, null, data)
//        kafkaProducer.send(record)
//        kafkaProducer.close()
//      }
//      .start()
//    query.awaitTermination()
//  }
//
//  def createKafkaProducer(bootstrapServers: String): KafkaProducer[String, String] = {
//    val props = new Properties()
//    props.put("bootstrap.servers", bootstrapServers)
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//    new KafkaProducer[String, String](props)
//  }
//}

package com.github.farrellw

import java.time.Duration
import java.util
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import net.liftweb.json._

object ScalaConsumer extends App {
  // Set constants
  val BootstrapServer = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"
  val Topic: String = "hr"

  implicit val formats: DefaultFormats.type = DefaultFormats

  // Set Properties to be used for Kafka COnsumer
  val properties = new Properties
  properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer)
  properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-application")

  // Initialize Kafka consumer and subscribe to topic
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)
  consumer.subscribe(util.Arrays.asList(Topic))


  while ( {
    true
  }) {
    // poll for new data
    val duration: Duration = Duration.ofMillis(100)
    val records: ConsumerRecords[String, String] = consumer.poll(duration)

    records.forEach((record: ConsumerRecord[String, String]) => {
      println("Hello World")
      // Retrieve the message from each record
      val message = record.value()
      println(s"Message Received: $message")

      // Parse the string into a scala case class
      try {
        val m = parse(record.value())
      } catch {
        case e: Exception => println("Failed parsing message into scala case class", e)
      }

      // More information
      println(s"Key: ${record.key}. Value: ${record.value}")
      println(s"Partition: ${record.partition}, Offset ${record.offset}")
    })
  }

  def parseIntoCaseClass(message: String): Unit ={

  }
}
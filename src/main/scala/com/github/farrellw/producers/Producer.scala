package com.github.farrellw.producers

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object Producer extends App {
  // Set constants
  val BootstrapServer = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"
  val Topic: String = "mytopic-name"

  // Set Properties to be used for Kafka Producer
  val properties = new Properties
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer)
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  //
  // create the producer
  val producer = new KafkaProducer[String, String](properties)

  val record = new ProducerRecord[String, String](Topic, "MESSAGE TO SEND")

  producer.send(record)

  producer.close()
}

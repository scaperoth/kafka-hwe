package com.github.farrellw.producers

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import faker._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import java.util.Properties

case class User(name: String, username: String, email: String)

object ProducerWithFaker extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  // Set constants
  val BootstrapServer = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"
  val Topic: String = "users"

  // Set Properties to be used for Kafka Producer
  val properties = new Properties
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BootstrapServer)
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  //
  // create the producer
  val producer = new KafkaProducer[String, String](properties)

  val recordsToCreate = 10
  val range = (1 to recordsToCreate).toList
  range.map(id => {
    val key = id.toString
    val name = Name.name
    val user = User(name, Internet.user_name(name), Internet.free_email(name))
    val jsonString = write(user)

    new ProducerRecord[String, String](Topic, key, jsonString)
  }).foreach(record => {

    producer.send(record, new Callback() {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        if (e == null) {
          println(s"Received new metadata. \n Topic: ${recordMetadata.topic()} \n Partition: ${recordMetadata.partition()} \n Offset: ${recordMetadata.offset()} \n Timestamp: ${recordMetadata.timestamp()}")
        }
        else println("Error while producing", e)
      }
    })
  })

  producer.close()
}

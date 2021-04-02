package com.github.farrellw.producers

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import faker._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import java.util.Properties

case class Person(name: String, phone: String, company: String, state: String, age: Int, house: String)

object ProducerWithFaker {
  implicit val formats: DefaultFormats.type = DefaultFormats
  val BootstrapServer = "35.239.241.212:9092,35.239.230.132:9092,34.69.66.216:9092"
  val Topic: String = "question-5"

  def main(args: Array[String]): Unit = {

    // Create the Kafka Producer
    val properties = getProperties(BootstrapServer)
    val producer = new KafkaProducer[String, String](properties)

    // create n fake records to send to topic
    val recordsToCreate = 2000
    val range = (1 to recordsToCreate).toList

    val houses = List("Gryffindor", "Hufflepuff", "Ravenclaw", "Slytherin")
    range.map(id => {
      val house = houses(id % 4)
      val r = new scala.util.Random

      val key = id.toString

      // Use the faker library ( https://github.com/bitblitconsulting/scala-faker ) to generate Users
      // User, as a case class, is defined at the top of this file
      val name = Name.name
      val user = Person(name, PhoneNumber.phone_number, Company.name, Address.state, r.nextInt(100), house)

      // write scala case class to a JSON string
      val jsonString = write(user)

      new ProducerRecord[String, String](Topic, key, jsonString)
    }).foreach(record => {

      // send records to topic
      producer.send(record, new Callback() {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (e == null) {
            println(
              s"""
                 |Sent Record: ${record.value()}
                 |Topic: ${recordMetadata.topic()}
                 |Partition: ${recordMetadata.partition()}
                 |Offset: ${recordMetadata.offset()}
                 |Timestamp: ${recordMetadata.timestamp()}
          """.stripMargin)
          } else println("Error while producing", e)
        }
      })
    })

    producer.close()
  }

  def getProperties(bootstrapServer: String): Properties = {
    // Set Properties to be used for Kafka Producer
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties
  }
}

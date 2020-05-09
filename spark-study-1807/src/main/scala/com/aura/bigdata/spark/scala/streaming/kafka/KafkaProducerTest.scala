package com.aura.bigdata.spark.scala.streaming.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * kafka的生产者
  */
object KafkaProducerTest {
    val array = Array(
        "hello you",
        "hello me",
        "hello hei",
        "hello you"
    )
    def main(args: Array[String]): Unit = {
        val properties = new Properties()
        properties.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092")
        properties.put("key.serializer", classOf[StringSerializer].getName)
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer:Producer[String, String] = new KafkaProducer(properties)
        for (i <- 0 until 99) {
            val producerRecord = new ProducerRecord[String, String]("test1", null, i + "", array(i % array.length))
            producer.send(producerRecord)
        }
        producer.close()
    }
}

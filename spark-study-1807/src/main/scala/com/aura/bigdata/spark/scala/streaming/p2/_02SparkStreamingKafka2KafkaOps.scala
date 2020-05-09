package com.aura.bigdata.spark.scala.streaming.p2

import java.util.Properties

import com.aura.bigdata.spark.java.streaming.MyKafkaProducer
import com.aura.bigdata.spark.scala.util.KafkaManager
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过应用案例，来完成（准时）实时数据清洗clean过程，
  * <<<!>>>3111<<<!>>>,<<<!>>>238<<<!>>>,<<<!>>>20181111132903<<<!>>>,<<<!>>>58.223.1.112<<<!>>>,<<<!>>>202.102.92.18<<<!>>>,<<<!>>>60726<<<!>>>,<<<!>>>80<<<!>>>,<<<!>>>www.sumecjob.com<<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>><<<!>>>,<<<!>>>http://www.sumecjob.com/Social.aspx<<<!>>>,<<<!>>>2556928113<<<!>>>
  * 访问通过flume采集进入kafka的src topic，接下来使用sparkStreaming程序来消费src topic中的数据，对其进行清洗，将上述数据转化为：
  *
  * 3111|238|20181111132903|58.223.1.112:60726|202.102.92.18:80|http://www.sumecjob.com/Social.aspx|2556928113
  *
  * 将进行清洗之后的结果反写到kafka的dest-topic中去。
  * flume-kafka 参见官网：http://flume.apache.org/FlumeUserGuide.html#kafka-sink
  *
  *
  * kryo序列化的数据不够稳定，会造成kafka的并发问题，所以转而使用java原生的Serializable操作
  *
  */
object _02SparkStreamingKafka2KafkaOps {

    val zkTopicOffsetPath = "/offsets"

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 4) {
            println(
                """
                  |Parameter Errors! Usage: <batchInterval> <groupId> <topics>
                  |batchInterval        : 批次间隔时间
                  |groupId              : 消费组的id
                  |src-topic            : 读取的topic
                  |dest-topic           : 写入的topic
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, group, srcTopic, destTopic) = args
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "auto.offset.reset"-> "smallest"//必须是smallest，不能largest
        )
        val conf = new SparkConf().setMaster("local[2]")
                .setAppName("_02SparkStreamingKafka2KafkaOps")
        val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))

        val messages = KafkaManager.createMessage(ssc, kafkaParams, srcTopic, group, client, zkTopicOffsetPath)


        val properties = new Properties()
        properties.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092")
        properties.put("key.serializer", classOf[StringSerializer].getName)
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer:MyKafkaProducer[String, String] = new MyKafkaProducer(properties)

        val producerBC:Broadcast[MyKafkaProducer[String, String]] = ssc.sparkContext.broadcast(producer)

        messages.foreachRDD(rdd => {
            if(!rdd.isEmpty()) {
                val cleanedRDD = rdd.map{case (key, msg) => {
                    val fields = msg.split("<<<!>>>,<<<!>>>")
                    if(fields == null || fields.length != 15) {
                        ""
                    } else {
                        val field1 = fields(0).replaceAll("<<<!>>>", "")
                        val field2 = fields(1)
                        val dateTime = fields(2)
                        val srcIpPort = fields(3) + ":" + fields(5)
                        val destIpPort = fields(4) + ":" + fields(6)
                        val url = fields(13)
                        val info = fields(14).replaceAll("<<<!>>>", "")
                        val result = s"${field1}|${field2}|${dateTime}|${srcIpPort}|${destIpPort}|${url}|${info}"
                        result
                    }
                }}
                //将操作完毕之后的数据写入到kafka中
/*                cleanedRDD.foreach(msg => {//这种操作方式，效率太低，抛弃
                    val properties = new Properties()
                    properties.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092")
                    properties.put("key.serializer", classOf[StringSerializer].getName)
                    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                    val producer:Producer[String, String] = new KafkaProducer(properties)
                    val record = new ProducerRecord[String, String](destTopic, msg)
                    producer.send(record)
                    producer.close()
                })*/
                /*cleanedRDD.foreachPartition(partition => {//方式二：
                    if(!partition.isEmpty) {
                        val properties = new Properties()
                        properties.put("bootstrap.servers", "bigdata01:9092,bigdata02:9092,bigdata03:9092")
                        properties.put("key.serializer", classOf[StringSerializer].getName)
                        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                        val producer:Producer[String, String] = new KafkaProducer(properties)
                        partition.foreach(msg => {
                            if(!msg.isEmpty) {
                                val record = new ProducerRecord[String, String](destTopic, msg)
                                producer.send(record)
                            }
                        })
                        producer.close()
                    }
                })*/

                cleanedRDD.foreach(msg => {
                    if(!msg.isEmpty) {
                        val p = producerBC.value
                        val record = new ProducerRecord[String, String](destTopic, msg)
                        p.send(record)
                    }
                })
            }
            //更新zk中的偏移量
            KafkaManager.storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, group, zkTopicOffsetPath, client)
        })
        ssc.start()
        ssc.awaitTermination()
    }

    val client = {//代码块-->zookeeper
        val client = CuratorFrameworkFactory.builder()
            .connectString("bigdata01:2181,bigdata02:2181,bigdata03:2181/kafka")
            .retryPolicy(new ExponentialBackoffRetry(1000, 3))
            .namespace("mykafka")
            .build()
        client.start()
        client
    }
}

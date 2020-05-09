package com.aura.bigdata.spark.scala.streaming.p1

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * 基于Receiver的方式去读取kafka中的数据
  *     1、为了保障数据零丢失，必须要开启wal预写日志，该机制必须要求有一个目录checkpoint来存放对应的数据
  *
  * [bigdata@bigdata01 kafka]$ bin/kafka-topics.sh --create --topic test1 --partitions 3 --replication-factor 3 --zookeeper bigdata01:2181/kafka
    Created topic "test1".
    [bigdata@bigdata01 kafka]$ bin/kafka-topics.sh --describe --topic test1 --zookeeper bigdata01:2181/kafka
    Topic:test1	PartitionCount:3	ReplicationFactor:3	Configs:
	Topic: test1	Partition: 0	Leader: 12	Replicas: 12,11,13	Isr: 12,11,13
	Topic: test1	Partition: 1	Leader: 13	Replicas: 13,12,11	Isr: 13,12,11
	Topic: test1	Partition: 2	Leader: 11	Replicas: 11,13,12	Isr

  制定的策略的问题
  */
object _04SparkKafkaCheckpointOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 5) {
            println(
                """
                  |Parameter Errors! Usage: <batchInterval> <zkQuorum> <groupId> <topics> <checkpoint>
                  |batchInterval        : 批次间隔时间
                  |zkQuorum             : zookeeper url地址
                  |groupId              : 消费组的id
                  |topic                : 读取的topic
                  |checkpoint           : checkpoint路径
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, zkQuorum, groupId, topic, checkpoint) = args

        val conf = new SparkConf().setMaster("local[2]").setAppName("_04SparkKafkaCheckpointOps")
        val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))
        ssc.checkpoint(checkpoint)//hdfs中
        val topics = topic.split(",").map((_, 3)).toMap
        /*
         ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
         */
        val kafkaParams = Map[String, String](
            "zookeeper.connect" -> "bigdata01:2181,bigdata02:2181,bigdata03:2181/kafka",
            "group.id" -> "test2",
            "auto.offset.reset"-> "largest"
        )
        val messages = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)

        messages.foreachRDD((rdd, bTime) => {
            if(!rdd.isEmpty()) {
                println("###########################->RDD count: " + rdd.count)
                println("###########################->RDD count: " + bTime)
            }
        })


        ssc.start()
        ssc.awaitTermination()
    }
}

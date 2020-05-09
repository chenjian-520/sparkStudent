package com.aura.bigdata.spark.scala.streaming.p1

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
  */
object _03SparkKafkaReceiverOps {
    def main(args: Array[String]): Unit = {
        if(args == null || args.length < 4) {
            println(
                """
                  |Parameter Errors! Usage: <batchInterval> <zkQuorum> <groupId> <topics>
                  |batchInterval        : 批次间隔时间
                  |zkQuorum             : zookeeper url地址
                  |groupId              : 消费组的id
                  |topic                : 读取的topic
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, zkQuorum, groupId, topic) = args

        val conf = new SparkConf().setMaster("local[2]").setAppName("_03SparkKafkaReceiverOps")
        val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))
        /*
            读取kafka中的数据
            ssc: StreamingContext,
            zkQuorum: String,
                bigdata01:2181,bigdata02:2181,bigdata03:2181/kafka
            groupId: String,
            topics: Map[String, Int],
                topic名称-->partition个数
            storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
         */
        val topics = topic.split(",").map((_, 3)).toMap
        val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
        //业务操作
        messages.print()

        ssc.start()
        ssc.awaitTermination()
    }
}

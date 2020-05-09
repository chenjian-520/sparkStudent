package com.aura.bigdata.spark.scala.streaming.p1

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object _05SparkKafkaDirectOps {
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
                  |checkpoint           : checkpoint
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, zkQuorum, groupId, topic, checkpoint) = args

        val conf = new SparkConf().setMaster("local[2]").setAppName("_05SparkKafkaDirectOps")

        val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))

        val topics = topic.split(",").toSet
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "auto.offset.reset"-> "largest"
        )
        ssc.checkpoint(checkpoint)
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        //业务操作
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

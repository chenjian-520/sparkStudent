package com.aura.bigdata.spark.scala.streaming.p1

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaUtils.getFromOffsets

import scala.collection.JavaConversions

/**
  * 使用的zookeeper来管理sparkdriver读取的offset偏移量
  * 将kafka对应的topic的offset保存到的路径
  *
  * 约定，offset的保存到路径
  * /xxxxx/offsets/topic/group/partition/
  *     0
  *     1
  *     2
  *
  * bigdata01:2181,bigdata02:2181,bigdata03:2181/kafka
  */
object _07SparkKafkaDriverHAZooKeeperOps {

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

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
        val Array(batchInterval, zkQuorum, group, topic) = args
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "auto.offset.reset"-> "smallest"
        )

        val conf = new SparkConf().setMaster("local[2]").setAppName("_06SparkKafkaDirectOps2")

        def createFunc():StreamingContext = {
            val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))
            //读取kafka的数据
            val messages = createMessage(ssc, kafkaParams, topic, group)
            //业务操作
            messages.foreachRDD((rdd, bTime) => {
                if(!rdd.isEmpty()) {
                    println("###########################->RDD count: " + rdd.count)
                    println("###########################->RDD count: " + bTime)
                    //所有的业务操作只能在这里完成 这里的处理逻辑和rdd的操作一模一样
                }
                //处理完毕之后将偏移量保存回去
                storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, topic, group)
            })
            ssc
        }

        //开启的高可用的方式 要从失败中恢复过来
        val ssc = StreamingContext.getActiveOrCreate(createFunc _)
        ssc.start()
        ssc.awaitTermination()
    }

    def storeOffsets(offsetRanges: Array[OffsetRange], topic: String, group: String): Unit = {
        val zkTopicPath = s"/offsets/${topic}/${group}"
        for (range <- offsetRanges) {//每一个range中都存储了当前rdd中消费之后的偏移量
            val path = s"${zkTopicPath}/${range.partition}"
            ensureZKExists(path)
            client.setData().forPath(path, (range.untilOffset + "").getBytes())
        }
    }
    /*
      * 约定，offset的保存到路径 ----->zookeeper
      * /xxxxx/offsets/topic/group/partition/
      *     0
      *     1
      *     2
     */
    def createMessage(ssc:StreamingContext, kafkaParams:Map[String, String], topic:String, group:String):InputDStream[(String, String)] = {
        //从zookeeper中读取对应的偏移量，返回值适应fromOffsets和flag(标志位)
        val (fromOffsets, flag)  = getFromOffsets(topic, group)

        var message:InputDStream[(String, String)] = null
        if(!flag) {
            //有数据-->zookeeper中是否保存了SparkStreaming程序消费kafka的偏移量信息
            //处理第一次以外，从这个接口读取kafka对应的数据
            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
            message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        } else {
            //第一次读取的时候
            message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
        }
        message
    }

    //从zookeeper中读取kafka对应的offset --->
    def getFromOffsets(topic:String, group:String): (Map[TopicAndPartition, Long], Boolean) = {
        ///xxxxx/offsets/topic/group/partition/
        val zkTopicPath = s"/offsets/${topic}/${group}"
        ensureZKExists(zkTopicPath)

        //如果有直接读取对应的数据
        val offsets = for{p <- JavaConversions.asScalaBuffer(
            client.getChildren.forPath(zkTopicPath))} yield {
//                p --->分区所对应的值
            val offset = client.getData.forPath(s"${zkTopicPath}/${p}")
            (TopicAndPartition(topic, p.toInt), new String(offset).toLong)
        }
        if(!offsets.isEmpty) {
            (offsets.toMap, false)
        } else {
            (offsets.toMap, true)
        }
    }



    def ensureZKExists(zkTopicPath:String): Unit = {
        if(client.checkExists().forPath(zkTopicPath) == null) {//zk中没有没写过数据
            client.create().creatingParentsIfNeeded().forPath(zkTopicPath)
        }
    }

    val client = {//代码块编程 zk(servlet)--->Curator(SpringMVC/Struts2)
        val client = CuratorFrameworkFactory.builder()
                    .namespace("mykafka")//命名空间就是目录意思
                    .connectString("bigdata01:2181,bigdata02:2181,bigdata03:2181/kafka")
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build()
        client.start()
        client
    }


}

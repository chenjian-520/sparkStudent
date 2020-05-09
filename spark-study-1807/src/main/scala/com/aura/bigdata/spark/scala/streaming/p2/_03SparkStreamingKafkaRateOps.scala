package com.aura.bigdata.spark.scala.streaming.p2

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用zk来管理的消费的偏移量，确保当SparkStreaming挂掉之后再重启的时候，
  * 能够从正确的offset偏移量的位置开始消费，而不是从头开始消费。
  *
  */
object _03SparkStreamingKafkaRateOps {

    val zkTopicOffsetPath = "/offsets"

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 3) {
            println(
                """
                  |Parameter Errors! Usage: <batchInterval> <groupId> <topics>
                  |batchInterval        : 批次间隔时间
                  |groupId              : 消费组的id
                  |topic                : 读取的topic
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, group, topic) = args
        val kafkaParams = Map[String, String](
            "bootstrap.servers" -> "bigdata01:9092,bigdata02:9092,bigdata03:9092",
            "auto.offset.reset"-> "smallest"//必须是smallest，不能largest
        )

        val conf = new SparkConf().setMaster("local[2]")
                    .setAppName("_03SparkStreamingKafkaRateOps")
                    .set("spark.streaming.kafka.maxRatePerPartition", "10")

        /**
          * 业务逻辑都在这个函数中来完成
          * @return
          */
        def createFunc(): StreamingContext = {
            val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))

            //1、创建kafka对应的message
            val message:InputDStream[(String, String)] = createMessage(ssc, kafkaParams, topic, group)

            //4、获得kafka message信息之后开始的各种操作
            message.foreachRDD(rdd => {
                if(!rdd.isEmpty()) {
                    println("##########^_^#########rdd'count: " + rdd.count())
                }
                //5、更新最新offset信息会zk中
                storeOffsets(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, group)
            })
            ssc
        }

        val ssc = StreamingContext.getActiveOrCreate(createFunc _)

        /**
          * 读取
          * 转化
          * output
          */
        ssc.start()
        ssc.awaitTermination()
    }
    /*
        将最新的offset信息更新回去
        OffsetRange就代表了读取到的偏移量的数据范围
     */
    def storeOffsets(offsetRanges:Array[OffsetRange], group:String): Unit = {
        for(offsetRange <- offsetRanges) {
            val partition = offsetRange.partition
            val topic = offsetRange.topic
            val offset = offsetRange.untilOffset
            val path = s"${zkTopicOffsetPath}/${topic}/${group}/${partition}"
            ensureZKPathExists(path)
            client.setData().forPath(path, (offset + "").getBytes())
        }
    }

    /*
        创建kafka对应的message
        分清楚，两种情况
            第一次消费的时候，从zk取不到对应的偏移量

            之后才可以从zk中取到对应partition的偏移量信息
     */
    def createMessage(ssc: StreamingContext, kafkaParams: Map[String, String], topic: String, group:String): InputDStream[(String,String)] = {
        //3、获取topic对应的offsets信息
        val (fromOffsets, flag) = getFromOffsets(topic, group)
        //2、搭建基础架构
        var message:InputDStream[(String, String)] = null
        if(flag) {//标记使用从zk中得到了对应partition偏移量信息，如果有为true
            val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
            message = KafkaUtils.createDirectStream[
                            String,
                            String,
                            StringDecoder,
                            StringDecoder,
                            (String,String)](
                        ssc, kafkaParams,
                        fromOffsets, messageHandler//该fromOffsets必须要自己从zk中读取
                    )
        } else {//没有得到，为false
            message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet)
        }
        message
    }

    /**
      * 获取对应topic中的每一个partition的偏移量信息
      */
    def getFromOffsets(topic:String, group:String):(Map[TopicAndPartition, Long], Boolean) = {

        val zkPath = s"${zkTopicOffsetPath}/${topic}/${group}"
        //读取offset信息
        ensureZKPathExists(zkPath)

        //读取该目录下面的所有的子节点信息
        import scala.collection.JavaConversions._
        val offsets = for{ p <- client.getChildren.forPath(zkPath)} yield {
            val offset = new String(client.getData.forPath(s"${zkPath}/${p}")).toLong
            (TopicAndPartition(topic, p.toInt), offset)
        }

        if(offsets.isEmpty) {//无偏移量信息
            (offsets.toMap, false)
        } else {//有偏移量信息
            (offsets.toMap, true)
        }
    }

    def ensureZKPathExists(path:String): Unit = {
        if(client.checkExists().forPath(path) == null) {//确保读取的目录一定是存在的
            //创建之
            client.create().creatingParentsIfNeeded().forPath(path)
        }
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
// /kafka/mykafka/offsets/${topic}/${group}/${partition}
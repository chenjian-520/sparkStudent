package com.aura.bigdata.spark.scala.streaming.p2.exactly

import java.sql.DriverManager

import com.aura.bigdata.spark.scala.util.KafkaManager
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * 为了保证的数据消费一致性语义，提供一个幂等操作
  */
object KafkaOffsetIdempotent {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]")

    val processingInterval = 2
    val brokers = "bigdata01:9092,bigdata02:9092,bigdata03:9092"
    val topic = "mytopic1"
    val group = "g-1807"
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,  "auto.offset.reset" -> "smallest")


    /*

       1. 创建测试的mysql数据库
       create database test;
       2. 建表
       create table myorders(name varchar(100), orderid varchar(100) primary key);
       3. 新建topic： mytopic1
         kafka-topics.sh --zookeeper bigdata01:2181/kafka --create --topic mytopic1 --partitions 3 --replication-factor 1
       4. 往mytopic1发送数据， 数据格式为 "字符,数字"  比如  abc,3
     */

    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    val zkTopicOffsetPath = "/offsets"
    val messages = KafkaManager.createMessage(ssc, kafkaParams, topic, group, client, zkTopicOffsetPath)
    val jdbcUrl =  "jdbc:mysql://localhost:3306/test"
    val jdbcUser = "root"
    val jdbcPassword = "sorry"

    messages.foreachRDD(rdd=>{
      rdd.map(x=>x._2).foreachPartition(partition =>{
        val dbConn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
        // upsert update insert
        partition.foreach(msg=>{
          val name = msg.split(",")(0)
          val orderid = msg.split(",")(1)
          val sql = s"insert into myorders(name, orderid) values ('$name', '$orderid') ON DUPLICATE KEY UPDATE name='${name}'"
          val pstmt = dbConn.prepareStatement(sql)
          pstmt.execute()
        })
        // dbConn.commit()
        dbConn.close()
      })
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

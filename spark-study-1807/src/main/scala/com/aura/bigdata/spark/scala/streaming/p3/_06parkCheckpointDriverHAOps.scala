package com.aura.bigdata.spark.scala.streaming.p3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  */
object _06parkCheckpointDriverHAOps {
    def main(args: Array[String]): Unit = {
        if(args == null || args.length < 4) {
            println(
                """
                  |Parameter Errors! Usage: <batchInterval> <zkQuorum> <groupId> <topics> <checkpoint>
                  |batchInterval        : 批次间隔时间
                  |host                 : host
                  |port                 : port
                  |checkpoint           : checkpoint路径
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, host, port, checkpoint) = args

        val conf = new SparkConf()
            .setAppName("_06parkCheckpointDriverHAOps")
        def createFunc(): StreamingContext = {
            val ssc = new StreamingContext(conf, Seconds(batchInterval.toLong))
            ssc.checkpoint(checkpoint)
            //hdfs中
            val messages = ssc.socketTextStream(host, port.toInt)

            messages.foreachRDD((rdd, bTime) => {
                if (!rdd.isEmpty()) {
                    println("###########################->RDD count: " + rdd.count)
                    println("###########################->RDD count: " + bTime)
                }
            })
            ssc
        }

        val ssc = StreamingContext.getOrCreate(checkpoint, createFunc _)
        ssc.start()
        ssc.awaitTermination()
    }
}

package com.aura.bigdata.spark.scala.streaming.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming的窗口window函数操作
  *     每隔M长的时间间隔，统计过去N长时间内产生的数据。
  * 必须要注意两个参数：
  *     1、windowLength（窗口长度）      ： N
  *     2、slidingInterval（滑动频率）   ： M
  *     M和N都必须是BatchInterval的整数倍
  *  Q&A
  *   1、为了保障的数据的安全，一般情况下都要开启checkpoint
  *   2、如果window长度过长，挤压的数据比较多，就容易导致OOM异常
  */
object _02SparkStreamingWindowOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 5) {
            println(
                """Parameter Errors! Usage: <host> <port> <batchInterval> <window> <sliding>
                  |host         :   连接主机名
                  |port         :   连接端口
                  |batchInterval:   批次提交间隔时间
                  |window       ：  窗口长度
                  |sliding      ：  滑动频率
                """.stripMargin)
            System.exit(-1)
        }
        val Array(host, port, batchInterval, window, sliding) = args
        val conf = new SparkConf().setMaster("local[2]").setAppName("_02SparkStreamingWindowOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint("E:/data/spark/streaming/chk-2")
        /**
          * 假如batchDuration是2秒钟，每隔2个时间单位，统计过去3个时间单位内产生的数据
          *
          */
        val lines = ssc.socketTextStream(host, port.toInt)

        val pairs = lines.flatMap(_.split("\\s+")).map((_, 1))

        val retByWindow = pairs.reduceByKeyAndWindow(
            (v1: Int, v2: Int) => v1 + v2,
            Seconds(window.toLong * batchInterval.toLong),
            Seconds(sliding.toLong * batchInterval.toLong)
        )

        retByWindow.print()

        ssc.start()
        ssc.awaitTermination()
    }
}

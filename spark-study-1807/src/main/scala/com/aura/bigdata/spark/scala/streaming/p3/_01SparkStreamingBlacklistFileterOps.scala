package com.aura.bigdata.spark.scala.streaming.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming的使用transform操作完成在线黑名单过滤操作
  * 用户apache
  *
  *
  *
  *
  *     27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/faq.gif HTTP/1.1##200##1127
  *110.52.250.126##2016-05-30 17:38:20##GET /data/cache/style_1_widthauto.css?y7a HTTP/1.1##200##1292
  *27.19.74.143##2016-05-30 17:38:20##GET /static/image/common/hot_1.gif HTTP/1.1##200##680
  * 以ip为黑名单过滤的条件
  * blacklist(27.19.74.143, 110.52.250.126)
  *
  * 学习了一个transform的transformation操作，主要就是完成处理那些dstream没有提供类似于rdd-to-rdd操作（主要就是dstream和rdd的join操作）
  */
object _01SparkStreamingBlacklistFileterOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 3) {
            println(
                """Parameter Errors! Usage: <host> <port> <batchInterval>
                  |host         :   连接主机名
                  |port         :   连接端口
                  |batchInterval:   批次提交间隔时间
                """.stripMargin)
            System.exit(-1)
        }
        val Array(host, port, batchInterval) = args
        val conf = new SparkConf().setMaster("local[2]").setAppName("_01SparkStreamingBlacklistFileterOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)

        //黑名单
        val blacklist = Array(
            ("27.19.74.143" -> true),
            ("110.52.250.126" -> true)
        )

        val blacklistRDD:RDD[(String, Boolean)] = ssc.sparkContext.parallelize(blacklist)

        val linesDStream = ssc.socketTextStream(host, port.toInt)

        val ip2InfoDStream:DStream[(String, String)] = linesDStream.map(line => {
            val ip = line.substring(0, line.indexOf("##"))
            val leftStr = line.substring(line.indexOf("##") + 2)
            (ip, leftStr)
        })

        /**
          * join
          *     交叉连接    --->笛卡尔积
          *     内连接     inner join （等值连接）
          *     外连接
          *         left/right outer join
          *         左/右表所有都显示，右/左表表能连接上的就显示，连接不上的就显示为null
          *         全连接
          *             不管左右能不能连接上，左右都显示，相当于left outer join+right outer join
          * map join（不等值连接）
          */
        val filteredInfoDStream:DStream[(String, String)] = ip2InfoDStream.transform(rdd => {
            if(rdd.isEmpty()) {
                ssc.sparkContext.emptyRDD
            } else {//黑名单过滤
                //左（ip， leftInfo） leftOuterJoin 右（ip， boolean）
                // （ip， （leftInfo， option[boolean]））
                val joinRDD:RDD[(String, (String, Option[Boolean]))] = rdd.leftOuterJoin(blacklistRDD)

                joinRDD.filter{case (ip, (leftStr, option)) => {
                    !option.isDefined
                }}.map{case (ip, (leftStr, option)) => (ip, leftStr)}
            }
        })

        filteredInfoDStream.print()

        ssc.start()
        ssc.awaitTermination()
    }
}

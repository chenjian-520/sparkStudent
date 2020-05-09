package com.aura.bigdata.spark.scala.streaming.p1



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * SparkStreaming的基本操作
  *
  *     入口：StreamingContext
  *  监听网络某端口实时产生的数据，socket，每2秒提交一次spark作业
  *  也就是每一次sparkStreaming计算的是每2秒内产生的数据
  *
  *  注意：
  *     如果我们使用local的方式启动Streaming程序，必须要是cpu core的个数大于等于2.
  *     因为我们streaming程序，必须要有一个cpu core分配个receiver去接收外部的数据(优先分配)，
  *     剩余的core才会被分配给计算程序来进行计算。
  *
  */
object _01SparkStreamingWCOps {
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
        val conf = new SparkConf().setMaster("local[2]").setAppName("_01SparkStreamingWCOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)

        //加载外部网络端口的数据
        val linesDStream:InputDStream[String] = ssc.socketTextStream(host, port.toInt)

//        linesDStream.print()

        val rbkDStream:DStream[(String, Int)] = linesDStream.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_+_)
        rbkDStream.print()
//        rbkDStream.updateStateByKey()
        //要想让sparkStreaming程序启动，必须要调用start()方法
        println("----------------start之前------------------")
        ssc.start()
//        rbkDStream.reduceByKey(_+_).print()//不支持这种做法、
        println("----------------start之后,awaitTermination之前-----")
//        ssc.stop(false)
        ssc.awaitTermination()
        println("----------------awaitTermination之后------------------")
    }
}

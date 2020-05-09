package com.aura.bigdata.spark.scala.streaming.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * updateStateByKey:统计截止到目前为止的key对应的状态
  *     统计截止到目前为止每一类商品的销售量：
  *         之前的每一类的商品销量+最新进来销量
  *             之前的每一类的商品销量+最新进来销量
  *     历史数据中：袜子、鞋子、票子、妹子
  *             新进来的数据：汉子、车子、鞋子
  * Note:为了存储历史状态信息，必须要开启checkpoint
  *     java.lang.IllegalArgumentException: requirement failed: The checkpoint directory has not been set.
  * 案例：统计截止到目前为止产生的数据对应的wordcount
  */
object _03SparkStreamingUpdateStateByKeyOps {
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
        val conf = new SparkConf().setMaster("local[2]").setAppName("_03SparkStreamingUpdateStateByKeyOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint("E:/data/spark/streaming/chk-3")
        val lines = ssc.socketTextStream(host, port.toInt)

        val pairs = lines.flatMap(_.split("\\s+")).map((_, 1))

//        pairs.reduceByKey(_+_)

        val usbDS:DStream[(String, Int)] = pairs.updateStateByKey[Int]((seq, history) => updateFunc(seq, history))

        usbDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

    /**
      *
      * @param newInput  当前批次进来的key对应的value的列表
      * @param historyState 历史数据状态
      */
    def updateFunc(newInput: Seq[Int], historyState: Option[Int]):Option[Int] = {
//        var sum = 0
//        for (i <- 0 until(newInput.size)) {
//            sum += newInput(i)
//        }
////        historyState.get
//        val historyValue = if(historyState.isDefined) {
//            historyState.get
//        } else {
//            0
//        }
//        sum += historyValue
//
//        Option[Int](sum)
        Option[Int](newInput.sum + historyState.getOrElse(0))
    }
}

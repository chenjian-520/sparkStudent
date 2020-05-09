package com.aura.bigdata.spark.scala.streaming.p1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkStreaming的基本操作
  *
  *     入口：StreamingContext
  *  监听文件目录或者HDFS，每2秒提交一次spark作业
  *  也就是每一次sparkStreaming计算的是每2秒内产生的数据
  * 注意：
  * 1）在HDFS中的所有目录下的文件，必须满足相同的格式，不然的话，不容易处理。
  *     必须使用移动或者重命名的方式，将文件移入目录。一旦处理之后，文件的内容及时改变，也不会再处理了。
        val array = Array(
            "hello you",
            "hello me",
            "hello hei",
            "hello you"
        )
        val bw = new BufferedWriter(new FileWriter("E:/data/spark/streaming/test/heihei.txt"))
        val random = new Random()
        for (i <- 0 until(10)) {
            bw.write(array(random.nextInt(array.length)))
            bw.newLine()
        }
        bw.close()
        这样SparkStreaming程序才可以监听得到新增加的的文件
    2）基于HDFS的数据结源读取是没有receiver的，因此不会占用一个cpu core。

  */
object _02SparkStreamingWCOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 2) {
            println(
                """Parameter Errors! Usage: <batchInterval> <path>
                  |batchInterval:   批次提交间隔时间
                  |path         :   文件路径
                """.stripMargin)
            System.exit(-1)
        }
        val Array(batchInterval, path) = args
        val conf = new SparkConf().setMaster("local[2]").setAppName("_02SparkStreamingWCOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)
        val lines = ssc.textFileStream(path)

        val retDStream = lines.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
        retDStream.print

        //要想让sparkStreaming程序启动，必须要调用start()方法
        println("----------------start之前------------------")
        ssc.start()
        ssc.awaitTermination()
        println("----------------awaitTermination之后------------------")
    }
}

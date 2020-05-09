package com.aura.bigdata.spark.scala.streaming.p3

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import com.aura.bigdata.spark.scala.streaming.p3._03SparkStreamingUpdateStateByKeyOps.updateFunc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver

/**
  * SparkStreaming自定义的Receiver，用于满足任意数据源
  * 以socket为例来说明
  * custom：
  *     1、复写一个类Receiver，实现其中的方法
  *     2、注册该类的实例
  *     3、调用
  */
object _05SparkStreamingCustomReceiverOps {
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
        val conf = new SparkConf().setMaster("local[2]").setAppName("_05SparkStreamingCustomReceiverOps")
        val batchDuration = Seconds(batchInterval.toLong)
        //入口
        val ssc = new StreamingContext(conf, batchDuration)
        ssc.checkpoint("E:/data/spark/streaming/chk-5")
        val lines = ssc.receiverStream(new CustomReceiver(host, port.toInt))

        val pairs = lines.flatMap(_.split("\\s+")).map((_, 1))

        val usbDS:DStream[(String, Int)] = pairs.updateStateByKey[Int]((seq, history) => updateFunc(seq, history))
        usbDS.print()

        ssc.start()
        ssc.awaitTermination()
    }
}

/**
  * 使用的CustomReceiver去模拟SocketReceiver
  * @param storageLevel
  */
class CustomReceiver(val hostname:String,
                     val port:Int,
                     storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2)
    extends Receiver[String](storageLevel) {

    private var socket:Socket = null;
    override def onStart(): Unit = {//开启相应的资源 thread buffer
        socket = new Socket(hostname, port)
        new Thread(new Runnable {
            override def run(): Unit = {
                val br = new BufferedReader(new InputStreamReader(socket.getInputStream))
                var line:String = null
                while((line = br.readLine()) != null) {
                    //存数据
                    store(line)
                }
            }
        }).start()
    }

    override def onStop(): Unit = {
        if(socket != null) {
            socket.close()
        }
    }

}
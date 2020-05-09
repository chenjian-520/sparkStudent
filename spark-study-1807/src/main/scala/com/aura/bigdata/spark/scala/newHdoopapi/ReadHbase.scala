package com.aura.bigdata.spark.scala.newHdoopapi

  import com.aura.bigdata.spark.scala.core.p1._01SparkWordCountApp
  import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
  import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
  import org.apache.hadoop.hbase.io.ImmutableBytesWritable
  import org.apache.hadoop.hbase.mapreduce.TableInputFormat
  import org.apache.spark.{SparkConf, SparkContext}
  //import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
  import org.apache.hadoop.hbase.mapred.TableOutputFormat
  import org.apache.hadoop.hbase.util.Bytes
  import org.apache.hadoop.mapred.JobConf
  //import org.apache.hadoop.mapreduce.Job
  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.SparkSession

  /**
    * Created by blockchain on 18-9-9 下午3:45 in Beijing.
    */
  class ReadHbase{
    def main(args: Array[String]): Unit = {

    }
  }


  object SparkHBaseRDD {
    def main(args: Array[String]) {
      // 屏蔽不必要的日志显示在终端上
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      val conf = new SparkConf()
      conf.setAppName(s"${_01SparkWordCountApp.getClass.getSimpleName}")
      conf.setMaster("local")
      val sc = new SparkContext(conf)
      val indataRDD = sc.makeRDD(Array("2,jack,16", "1,Lucy,15", "5,mike,17", "3,Lily,14"))
      println("----------")
      val rdd = indataRDD.map(_.split(',')).map{ arr=>
        /*一个Put对象就是一行记录，在构造方法中指定主键
         * 所有插入的数据 须用 org.apache.hadoop.hbase.util.Bytes.toBytes 转换
         * Put.addColumn 方法接收三个参数：列族，列名，数据*/
        val put = new Put(Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
        (new ImmutableBytesWritable, put)

      }

      val rdd1 = indataRDD.map(_.split(',')).foreach(arr => {
       for (i <- 0 until arr.size){
         println(arr(i))
       }
      })

    }
  }


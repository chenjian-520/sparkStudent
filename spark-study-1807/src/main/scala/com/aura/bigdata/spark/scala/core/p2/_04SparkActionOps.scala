package com.aura.bigdata.spark.scala.core.p2

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
    1、reduce：
        执行reduce操作，返回值是一个标量
    2、collect： 慎用
        将数据从集群中的worker上拉取到dirver中，所以在使用的过程中药慎用，意外拉取数据过大造成driver内存溢出OOM(OutOfMemory)
        NPE(NullPointerException)
        所以在使用的使用，尽量使用take，或者进行filter再拉取
    3、count：返回当前rdd中有多少条记录
        select count(*) from tbl;
    4、take：take(n)
        获取rdd中前n条记录
        如果rdd数据有序，可以通过take(n)求TopN
    5、first:take(1)
        获取rdd中的第一条记录
    6、saveAsTextFile：
        将rdd中的数据保存到文件系统中
    7、countByKey：和reduceByKey效果相同，但reduceByKey是一个Transformation
        统计相同可以出现的次数，返回值为Map[String, Long]
    8、foreach：略 遍历
 */
object _04SparkActionOps extends App {
//    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.project-spark").setLevel(Level.WARN)
    val conf = new SparkConf()
        .setMaster("local[2]")
        .setAppName(s"${_03SparkTransformationOps.getClass.getSimpleName}")
    val sc = new SparkContext(conf)

    val list = List(
        "5  刘帆 1807bd-xa",
        "2  王佳豪 1807bd-bj",
        "8  邢宏 1807bd-xa",
        "3  刘鹰 1807bd-sz",
        "4  宋志华 1807bd-wh",
        "1  郑祥楷 1807bd-bj",
        "7  张雨 1807bd-bj",
        "6  何昱 1807bd-xa"
    )

    val listRDD: RDD[String] = sc.parallelize(list)
    val list1 = 1 to 9
    val listRDD1 = sc.parallelize(list1)
    //reduce:统计rdd中的和
    val 和 = listRDD1.reduce((v1, v2) => v1 + v2)
    println(和)
    //2 count
    println(s"listRDD1的条数：${listRDD1.count()}")
    // 3 take 获取rdd中的n条记录
    val take: Array[Int] = listRDD1.take(3)
    println(take.mkString("[", ",", "]"))
    private val first: Int = listRDD1.first()
    println(first)
    val top3:Array[Int] = listRDD1.takeOrdered(3)(new Ordering[Int]{
        override def compare(x: Int, y: Int) = {
            y.compareTo(x)
        }
    })
    println(top3.mkString("[", ",", "]"))
    println("---------------saveAsTextFile---------------------")
//    listRDD.saveAsTextFile("E:/data/spark/out")
    listRDD.saveAsObjectFile("E:/data/spark/out1")
    println("---------------saveAsHadoop/SequenceFile---------------------")
    val cid2InfoRDD:RDD[(String, String)] = listRDD.map{case line => {
        val fields = line.split("\\s+")
        (fields(2), line)
    }}

    /**
      * saveAsHadoopFile()  ---> org.apache.hadoop.mapred.OutputFormat 借口
      * saveAsNewAPIHadoopFile() --> org.apache.hadoop.mapreduce.OutputFormat 抽象类
      * job.setOutputFormat(xxx.classs)
      *
      *
      *  path: String   --->将rdd数据存储的目的地址
         keyClass: Class[_], mr中输出记录包含key和value，需要指定对应的class
         valueClass: Class[_],
         job.setOutputKeyClass()
         job.setOutputValueClass()
         outputFormatClass: Class[_ <: NewOutputFormat[_, _]] 指定对应的Format来完成数据格式化输出
         TextOutputFormat
      */
    cid2InfoRDD.saveAsNewAPIHadoopFile(
        "E:/data/spark/out2",
        classOf[Text],
        classOf[Text],
        classOf[TextOutputFormat[Text, Text]]
    )

    //countByKey作用就是记录每一个key出现的次数，作用同reduceByKey(_+_)
    private val cid2Count: collection.Map[String, Long] = cid2InfoRDD.countByKey()

    for((k, v) <- cid2Count) {
        println(k + "---" + v)
    }

    sc.stop()
}

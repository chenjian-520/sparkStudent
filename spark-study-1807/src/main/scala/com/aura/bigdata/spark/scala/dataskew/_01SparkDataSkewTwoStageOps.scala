package com.aura.bigdata.spark.scala.dataskew

import com.aura.bigdata.spark.scala.util.SparkUtil
import org.apache.log4j.{Level, Logger}

import scala.util.Random

/**
  * 对于xxxByKey等聚合类算子，比如groupByKey，reduceByKey，aggregateByKey等待操作的数据倾斜
  * 两阶段聚合是非常有效果
  * 局部聚合：
  *     就是将原本数据量非常大的key，添加随机前缀进行拆分，然后对拆分之后的数据进行统计
  * 全局聚合
  *     对聚合聚合的结果去掉前缀，然后进行全局统计
  * 对于下面的操作，可以进行优化：
  * 将发生数据倾斜的key和没有发生的分开来进行计算
  *     filter
  */
object _01SparkDataSkewTwoStageOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val sc = SparkUtil.sparkContext("local[2]", "_01SparkDataSkewTwoStageOps")
        val list = List(
            "hello you hello hello me",
            "hello you hello hello shit",
            "oh hello she study"
        )

        val listRDD = sc.parallelize(list)

        val pairsRDD = listRDD.flatMap(line => line.split("\\s+")).map((_, 1))
        //step 1 找到发生数据倾斜key
        val sampleRDD = pairsRDD.sample(false, 0.6)
        val cbk= sampleRDD.countByKey()
//        cbkRDD.foreach(println)
        val sortedInfo = cbk.toBuffer.sortWith((t1, t2) => t1._2 > t2._2)
        val dataSkewKey = sortedInfo.head._1
//        sortedInfo.foreach(println)
        println("发生了数据倾斜的Key：" + dataSkewKey)
        //step 2 给对应的key打上N以内的随机前缀
        val prefixPairsRDD = pairsRDD.map{case (word, count) => {
            if(word.equals(dataSkewKey)) {
                val random = new Random()
                val prefix = random.nextInt(2)//0 1
                (s"${prefix}_${word}", count)
            } else {
                (word, count)
            }
        }}
        prefixPairsRDD.foreach(println)
        //step 3 局部聚合
        val partAggrInfo = prefixPairsRDD.reduceByKey(_+_)
        println("===============>局部聚合之后的结果：")
        partAggrInfo.foreach(println)
        //step 4 全局聚合
        //step 4.1 去掉前缀
        val unPrefixPairRDD = partAggrInfo.map{case (word, count) => {
            if(word.contains("_")) {
                (word.substring(word.indexOf("_") + 1), count)
            } else {
                (word, count)
            }
        }}
        println("================>去掉随机前缀之后的结果：")
        unPrefixPairRDD.foreach(println)
        // step 4.2 全局聚合
        val fullAggrInfo = unPrefixPairRDD.reduceByKey(_+_)
        println("===============>全局聚合之后的结果：")
        fullAggrInfo.foreach(println)
        sc.stop()
    }
}

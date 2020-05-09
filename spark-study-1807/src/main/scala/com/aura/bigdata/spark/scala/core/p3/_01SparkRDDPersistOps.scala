package com.aura.bigdata.spark.scala.core.p3

import com.aura.bigdata.spark.scala.core.p2._03SparkTransformationOps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 持久化策略使用
  * 需求：比较序列化之后的计算和没有经过序列化之后的计算
  *     读取数据：E:/data/spark/core/sequences.txt
  */
object _01SparkRDDPersistOps {
    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_01SparkRDDPersistOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        var start = System.currentTimeMillis()
        val lines = sc.textFile("file:///E:/data/spark/core/sequences.txt")
        val count = lines.count()
        println("没有序列化之后计算的数据的条数：" + count + "消耗时间：" + (System.currentTimeMillis() - start) + "ms")
        //序列化
//        lines.cache()
        lines.persist(StorageLevel.MEMORY_ONLY)
        start = System.currentTimeMillis()
        val count1 = lines.count()
        println("持久化之前计算的数据的条数：" + count1 + "消耗时间：" + (System.currentTimeMillis() - start) + "ms")
        //使用完毕之后卸载持久化的数据
        lines.unpersist()
        sc.stop()
    }
}

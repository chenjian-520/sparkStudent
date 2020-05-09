package com.aura.bigdata.spark.scala.core.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 使用combineByKey和aggregateByKey来模拟groupByKey和reduceByKey
  * 不管combineByKey还是aggregateByKey底层都是使用combineByKeyWithClassTag来实现的
  *
  * 这两个有啥区别？
  *
  *
  *
  *
  */
object _02SparkTransformationOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_02SparkTransformationOps.getClass.getSimpleName}")
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
//        cbk2rbk(sc, list)
        cbk2gbk(sc, list)

        sc.stop()
    }

    def cbk2gbk(sc:SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2Info:RDD[(String, String)] = listRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }}

        val cbk2gbk:RDD[(String, ArrayBuffer[String])] = cid2Info.combineByKey(
            line => ArrayBuffer[String](line),
            (ab:ArrayBuffer[String], line:String) => {
                ab.append(line)
                ab
            },
            (ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]) => {
                ab1.appendAll(ab2)
                ab1
            }
        )
        cbk2gbk.foreach(println)
    }
    /**
      * 使用combineByKey来模拟reduceByKey
      * @param sc
      * @param list
      */
    def cbk2rbk(sc:SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2Info:RDD[(String, String)] = listRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }}
        cid2Info.foreachPartition(patition => {
            patition.foreach(println)
        })
        val cid2Counts = cid2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)
        cid2Counts.foreach(println)
    }

    /**
      * V是被聚合的rdd中k-v键值对中的value的类型
      * C是经过聚合操作之后又v转化成的类型
      * 当前方法，rdd中一个key，在一个分区中，只会创建/调用一次，做数据类型初始化
      * 比如：hello这个key，在partition0和partition1中都有出现
      * 在partition0中聚合的时候createCombiner之被调用一次
      *
      * @return
      */
    def createCombiner(line:String):Int = {
        val fields = line.split("\\s+")
        println("-----createCombiner---> " + fields(2))
        1
    }

    /**
      * scala代码，求1+。。。+10
      * var sum = 0
      * for(i <- 1 to 10) {
      *     sum = sum + i
      * }
      * println(sum)
      * mergeValue就类似于上面的代码
      * 合并同一个分区中，相同key对应的value数据，在一个分区中，相同的key会被调用多次
      * @return
      */
    def mergeValue(sum:Int, line:String):Int = {
        val fields = line.split("\\s+")
        println(">>>-----mergeValue---> " + fields(2))
        sum + 1
    }

    /**
      * 相同key对应分区间的数据进行合并
      * @return
      */
    def mergeCombiners(sum1:Int, sum2:Int):Int = {
        println(">>>-----mergeCombiners--->>> sum1: " + sum1 + "--->sum2: " + sum2)
        sum1 + sum2
    }
}

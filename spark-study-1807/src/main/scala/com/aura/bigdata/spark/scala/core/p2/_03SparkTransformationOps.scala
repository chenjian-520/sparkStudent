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
  * 1、本质上combineByKey和aggregateByKey都是通过combineByKeyWithClassTag来实现的，只不过实现的细节或者方式不大一样。
  * 2、combineByKey更适合做聚合前后数据类型不一样的操作，aggregateByKey更适合做聚合前后数据类型一致的操作
  *  因为我们可以在combineByKey提供的第一个函数中完成比较复杂的初始化操作，而aggregateByKey的第一个参数是一个值
  * 3、我们使用时最简单的版本，而在实际生产过程中，一般都是相对比较复杂的版本，还有其它参数的，比如partitioner，
  * mapSideCombine。
  *     partitioner制定并行度，
  *     mapSideCombine控制是否执行本地预聚合
  *
  */
object _03SparkTransformationOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
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
//        abk2rbk(sc, list)
        abk2gbk(sc, list)

        sc.stop()
    }

    def abk2gbk(sc:SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2Info:RDD[(String, String)] = listRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }}

        val abk2gbk:RDD[(String, ArrayBuffer[String])] = cid2Info.aggregateByKey(ArrayBuffer[String]())(
            (ab:ArrayBuffer[String], line:String) => {
                ab.append(line)
                ab
            },
            (ab1:ArrayBuffer[String], ab2:ArrayBuffer[String]) => {
                ab1.appendAll(ab2)
                ab1
            }
        )
//        abk2gbk.foreach(println)
    }

    /**
      * 使用aggregateByKey来模拟reduceByKey
      * @param sc
      * @param list
      */
    def abk2rbk(sc:SparkContext, list:List[String]): Unit = {
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2Info:RDD[(String, String)] = listRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }}

        val cid2rbkRDD:RDD[(String, Int)] = cid2Info.aggregateByKey(0)(
            (sum:Int, line:String) => {
                sum + 1
            },
            (sum1:Int, sum2:Int) => {
                sum1 + sum2
            }
        )
        cid2rbkRDD.foreach(println)
    }

}

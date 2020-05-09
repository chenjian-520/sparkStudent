package com.aura.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Spark的分组排序
  * chinese ls 91
    english ww 56
    chinese zs 90
    chinese zl 76
    english zq 88
    chinese wb 95

        需求：求出各科成绩排名前三的学员信息

  */
object _07SparkGroupSortOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_07SparkGroupSortOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        val linesRDD = sc.textFile("data/topn.txt")

        val course2Info:RDD[(String, String)] = linesRDD.map(line => {
            val fields = line.split("\\s+")
            val course = fields(0)
            val value = fields(1) + " " + fields(2)
            (course, value)
        })

        //使用combineByKey来完成分组排序的操作
        val course2Infos = course2Info.combineByKey(createCombiner, mergeValue, mergeCombiners)

        course2Infos.foreach(println)


        sc.stop()
    }
    //初始化的操作
    def createCombiner(info: String):mutable.TreeSet[String] = {
        var ts = new mutable.TreeSet[String]()(new Ordering[String](){
            override def compare(x: String, y: String) = {
                val xFields = x.split("\\s+")
                val yFields = y.split("\\s+")
                val xScore = xFields(1).toInt
                val yScore = yFields(1).toInt
                yScore.compareTo(xScore)
            }
        })
        ts.add(info)
        ts
    }

    /**
      * 同一key，在同一partition分区中的聚合操作
      */
    def mergeValue(ts:mutable.TreeSet[String], info:String):mutable.TreeSet[String] = {
        ts.add(info)
        if(ts.size > 3) {
            ts.dropRight(1)
        } else {
            ts
        }
    }

    /**
      * 同一key，在两个partition分区间的聚合操作
      * @param ts1 分区一中的聚合的结果
      * @param ts2 分区二中的聚合的结果
      * @return
      */
    def mergeCombiners(ts1:mutable.TreeSet[String], ts2:mutable.TreeSet[String]):mutable.TreeSet[String] = {
        var ts = ts1.++(ts2)
        if (ts.size > 3) {
            ts.dropRight(ts.size - 3)
        } else {
            ts
        }
    }
}

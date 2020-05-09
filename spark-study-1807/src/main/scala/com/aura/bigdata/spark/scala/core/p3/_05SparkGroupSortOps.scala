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
object _05SparkGroupSortOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_05SparkGroupSortOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        val linesRDD = sc.textFile("data/topn.txt")

        val course2Info:RDD[(String, String)] = linesRDD.map(line => {
            val fields = line.split("\\s+")
            val course = fields(0)
            val value = fields(1) + " " + fields(2)
            (course, value)
        })

        val course2Infos:RDD[(String, Iterable[String])] = course2Info.groupByKey()
        val course2SortedInfo:RDD[(String, mutable.TreeSet[String])] = course2Infos.map{case (course, infos) => {
            var ts = new mutable.TreeSet[String]()(new Ordering[String](){
                override def compare(x: String, y: String) = {
                    val xFields = x.split("\\s+")
                    val yFields = y.split("\\s+")
                    val xScore = xFields(1).toInt
                    val yScore = yFields(1).toInt
                    yScore.compareTo(xScore)
                }
            })
            for(info <- infos) {
                ts.add(info)
                if(ts.size > 3) {
                    ts = ts.dropRight(1)
                }
            }
            (course, ts)
        }}
        course2SortedInfo.foreach{case (course, ts) => {
            for (info <- ts) {
                println(course + "---" + info)
            }
        }}
        sc.stop()
    }
}

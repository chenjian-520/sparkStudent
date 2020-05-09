package com.aura.bigdata.spark.scala.core.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/*
    数据：两列内容使用' '隔开
    20 21
    50 51
    50 52
    50 53
    50 54
    需求，对上述数据进行排序，首先按照第一列的正序排列，当字段相同的时候，在按照第二列的降序排列
        需求升级：第一列降序，第二列升序
 */
object _04SparkSecondSortOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_04SparkSecondSortOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/secondsort.csv")

//        val sbkRDD:RDD[(SecondSort, Int)] = lines.map(line => {
//            val fields = line.split("\\s+")
//            val first = fields(0).toInt
//            val second = fields(1).toInt
//            val ss = new SecondSort(first, second)
//            (ss, first)
//        }).sortByKey(numPartitions = 1)
//        sbkRDD.foreach{case (ss, first) => {
//            println(ss.first + "\t" + ss.second)
//        }}
        val sortedLines:RDD[String] = lines.sortBy(line => {
                val fields = line.split("\\s+")
                val first = fields(0).toInt
                val second = fields(1).toInt
                new SecondSort(first, second)
            }, ascending = false, numPartitions = 1)
        (new Ordering[SecondSort](){
            //第一列降序，第二列升序
            override def compare(x: SecondSort, y: SecondSort) = {
                var ret = y.first.compareTo(x.first)
                if(ret == 0) {
                    ret = x.second.compareTo(y.second)
                }
                ret
            }
        },
        ClassTag.Object.asInstanceOf[ClassTag[SecondSort]]
        )

        sortedLines.foreach(println)

        sc.stop()
    }
}

class SecondSort(val first:Int, val second:Int) extends Serializable with Ordered[SecondSort]/*Comparable[SecondSort]*/ {
    /**
      * 首先按照第一列的正序排列，当字段相同的时候，在按照第二列的降序排列
      */
   /* override def compareTo(o: SecondSort) = {
        var ret = first.compareTo(o.first)
        if(ret == 0) {
            ret = o.second.compareTo(this.second)
        }
        ret
    }*/

    override def compare(that: SecondSort) = {
        var ret = first.compareTo(that.first)
        if(ret == 0) {
            ret = that.second.compareTo(this.second)
        }
        ret
    }
}

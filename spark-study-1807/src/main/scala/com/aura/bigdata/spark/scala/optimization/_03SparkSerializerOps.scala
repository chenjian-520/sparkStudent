package com.aura.bigdata.spark.scala.optimization

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

        spark-submit 数据本地性设置
        --conf spark.locality.wait.process=3s \
        --conf spark.locality.wait.node=6s \
        --conf spark.locality.wait.rack=9s \
 */
object _03SparkSecondSortOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_03SparkSecondSortOps.getClass.getSimpleName}")
            //使用kryo的序列化方式
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[SecondSort]))
            .set("spark.locality.wait.process", "3s")

        val sc = new SparkContext(conf)

        val lines = sc.textFile("data/secondsort.csv")

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

class SecondSort(val first:Int, val second:Int) extends Ordered[SecondSort]{
    override def compare(that: SecondSort) = {
        var ret = first.compareTo(that.first)
        if(ret == 0) {
            ret = that.second.compareTo(this.second)
        }
        ret
    }
}

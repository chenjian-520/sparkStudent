package com.aura.bigdata.spark.scala.core.p1

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  */
object _02SparkWordCountApp {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName(s"${_02SparkWordCountApp.getClass.getSimpleName}")
            .master("local")
            .getOrCreate()
        val sc = spark.sparkContext

        val linesRDD = sc.textFile("F:\\spark阶段 old李\\Spark12\\代码\\spark-study-1807\\data\\sql\\teacher_basic.txt")

        linesRDD.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_).foreach(println)

        spark.stop()
    }
}

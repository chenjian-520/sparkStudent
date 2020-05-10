package com.ghgj.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object text4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("text02").setMaster("local[2]")

    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("file:///F:/ScalaWorkplace/Spark01/data/lagou.txt")
    val data1 = lines.map(line => {
      val arr = line.split("\\^")
      val a = arr(3)
      val b = arr(0) + " " + arr(1) + " " + arr(2) + " " + arr(3) + " " + arr(4) + " " + arr(5) + " " + arr(6) + " " + arr(7) + " " + arr(8) + " " + arr(9)
      (a, b )
    })

    data1.combineByKey(
      line =>"1",
      (ab: String, line: String) => {
        ab
      },
      (sum1: String, sum2: String) => {
        sum1 + sum2
      }
    )


  }
}

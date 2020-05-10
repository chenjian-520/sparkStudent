package com.ghgj.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object text3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("text02").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///F:/ScalaWorkplace/Spark01/data/lagou.txt")
    val data1 = lines.map(line => {
      val arr = line.split("\\^")
      val a = arr(1)
      val b = arr(0) + " " + arr(1) + " " + arr(2) + " " + arr(3) + " " + arr(4) + " " + arr(5) + " " + arr(6) + " " + arr(7) + " " + arr(8) + " " + arr(9)
      (a, b)
    })
    val unit = data1.filter(x => {
      if (x._1.contains("大数据") || x._1.contains("hadoop") || x._1.contains("spark")) {
        true
      } else {
        false
      }
    })
    //创建一个累加器
    val countAccu = sc.longAccumulator("localAccu")
    val unit1 = unit.map((x) => {
      val arr = x._2.split(" ")
      val a = arr(6)
      val b = x._2
      (a, b)
    }).map(x => {
      if (x._1.contains("本科") || x._1.contains("大专") || x._1.contains("硕士") || x._1.contains("博士") || x._1.contains("不限")) {
        countAccu.add(1L)
      }
      (x._1, x._2)
    }).combineByKey(
      line => 1,
      (ab: Int, line: String) => {
        ab + 1
      },
      (sum1: Int, sum2: Int) => {
        sum1 + sum2
      }
    )
    unit1.foreach(
      x=>{
        if (x._1.contains("本科")||x._1.contains("大专")||x._1.contains("硕士")||x._1.contains("博士")||x._1.contains("不限")){
          println(x)
        }
      }
    )
    println("总人数"+countAccu.value)



  }
}

package com.ghgj.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object text2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("text02").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///F:/ScalaWorkplace/Spark01/data/lagou.txt")
    val data1 = lines.map(line => {
      val arr = line.split("\\^")
      val a = arr(6) + " " + arr(7)
      val b = arr(0) + " " + arr(1) + " " + arr(2) + " " + arr(3) + " " + arr(4) + " " + arr(5) + " " + arr(6) + " " + arr(7) + " " + arr(8) + " " + arr(9)
      (a, b)
    })
    val unit = data1.combineByKey(
      line => 1,
      (ab: Int, line: String) => {
        ab+1
      },
      (ab1: Int, ab2: Int) => {
        ab1 + ab2
      }
    ).sortBy(x=>x._2,false,1)
    var a = 0
    unit.foreach(x=>{
      a = a+1;
      if(a==1){
        println(x)
      }
    })

  }
}

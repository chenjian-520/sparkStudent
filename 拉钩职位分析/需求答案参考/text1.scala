package com.ghgj.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}



object text1{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(s"${_05SparkGroupSortOps.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("file:///F:/ScalaWorkplace/Spark01/data/lagou.txt")
    val data1 = lines.map(line => {
      val arr = line.split("\\^")
      val a= arr(2)
      val b =arr(0)+" "+arr(1)+" "+arr(2)+" "+arr(3)+" "+arr(4)+" "+arr(5)+" "+arr(6)+" "+arr(7)+" "+arr(8)+" "+arr(9)
      (a,b)
    })
      var a =0
      val unit = data1.aggregateByKey(0)(
         (sum: Int, lnes:String) => {
           sum + 1
         },
         (sum1: Int, sum2: Int) => {
           sum1 + sum2
         }).sortBy(t=>t._2,true,1).foreach(x=>{
         a=a+1
        if(a<=10){
          println(x)
        }
      })
    sc.stop()



  }



}

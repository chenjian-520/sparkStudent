package com.aura.bigdata.spark.scala.core.p1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  */
object _01SparkWordCountApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName(s"${_01SparkWordCountApp.getClass.getSimpleName}")
        conf.setMaster("local")
        val sc = new SparkContext(conf)
        val linesRDD = sc.textFile("hdfs://ns1/data/hello")
        val wordsRDD = linesRDD.flatMap(line => line.split("\\s+"))
        val pairsRDD = wordsRDD.map(word => (word, 1))
        val rbkRDD = pairsRDD.reduceByKey((v1, v2) => v1 + v2)
        rbkRDD.foreach(p => println(p._1 + "===>" + p._2))

        sc.stop()
    }
}

package com.aura.bigdata.spark.scala.core.p4

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 广播变量之累加器
  * 类似mr中的Counter
  * 计算每个单词出现的次数的同时，计算单词local出现的次数
  *
  * 需要注意的地方：
  *     1、累加器的值得获取只能在dirver中来进行获取,在transformation中不建议获取
  *     2、累加器的执行，必须要依赖于一个action的触发，而且累加器值得获取只能在该action触发之后获取
  *     3、重复触发相关action，会造成累加器的值重复计算，所以在使用过程中要尽量避免
  *         可以通过累加器的重置解决该问题：accumulator.reset()
  *     4、如果这些累加器满足不了需要的话，请尝试自定义累加器
  *         作业：
  *             1、不仅仅要统计local出现的次数，还有统计from，so出现的次数，请注意，使用累加器
  *             使用自定义累加器解决该问题
  */
object SparkRDDAccumulatorOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SparkRDDAccumulatorOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        //创建一个累加器
        val myAccu = new MyAccumulator()
        sc.register(myAccu, "countAccu")//注册

        val linesRDD:RDD[String] = sc.textFile("data/10w-line-data.txt")
        val wordsRDD:RDD[String] = linesRDD.flatMap(_.split("\\s+"))
        val pairsRDD:RDD[(String, Int)] = wordsRDD.map(word => {
            if (word == "local") {
                myAccu.add("local")
            } else if(word == "and") {
                myAccu.add("add")
            }
            (word, 1)
        })//.map((_, 1))
        val rbkRDD:RDD[(String, Int)] = pairsRDD.reduceByKey(_+_)
        rbkRDD.count()
        println("myAccu: " + myAccu.value)
        Thread.sleep(1000000)
        sc.stop()
    }
}

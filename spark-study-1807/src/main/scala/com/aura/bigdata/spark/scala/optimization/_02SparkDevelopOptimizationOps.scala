package com.aura.bigdata.spark.scala.optimization

import com.aura.bigdata.spark.scala.util.SparkUtil
import org.apache.log4j.{Level, Logger}

/**
  * Spark开发调优二
  */
object _02SparkDevelopOptimizationOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val sc = SparkUtil.sparkContext("local[2]", "_02SparkDevelopOptimizationOps")

        val range = 1 to 100000
        val listRDD = sc.parallelize(range, 5)
        println("partition's " + listRDD.getNumPartitions)
        //过滤掉被3整除的数字
        val fiteredRDD = listRDD.filter(num => num % 3 != 0)

        val repatitionRDD = fiteredRDD.coalesce(3, shuffle = false)

        println("repatitionRDD' partition size: " + repatitionRDD.getNumPartitions)

        sc.stop()
    }
}

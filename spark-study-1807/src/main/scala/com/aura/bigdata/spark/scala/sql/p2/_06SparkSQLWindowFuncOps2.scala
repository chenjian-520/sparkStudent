package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 开窗函数使用之
  *      sum() over()         --->分组累加
  *   数据：
  *     {"product_code":1438, "event_date": "2016-05-13", "duration": 165}
  *     需要统计用户的总使用时长（累加历史）
  *     只统计昨天和今天
  */
object _06SparkSQLWindowFuncOps2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_06SparkSQLWindowFuncOps2")
            //            .enableHiveSupport()
            .getOrCreate()
        val productDF = spark.read.json("data/sql/product_info.json")
        println("==================原始数据=========================")
        productDF.show()
        productDF.createOrReplaceTempView("product")
        println("==================每一个产品的用户使用累加历史时长=========================")
        val sumHistorySQL =
            """
              |select
              |  product_code,
              |  event_date,
              |  duration,
              |  sum(duration) over(partition by product_code order by event_date) sum_history_duration
              |from product
            """.stripMargin
        spark.sql(sumHistorySQL).show()
        spark.stop()
    }
}

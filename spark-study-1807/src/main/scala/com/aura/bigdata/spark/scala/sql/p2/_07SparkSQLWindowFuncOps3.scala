package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 开窗函数使用之
  *      max/min() over()         --->分组累加
  *  {"course": "chinese", "name":"ls", "score": 91}
     {"course": "english", "name":"ww", "score": 56}
  *   计算各个科目成绩最高的同学，成绩最低的同学，平均值
  *
  *   select
  *     course,
  *     max(score),
  *     min(score)
  *   from score
  *   group by course
  *
  *   select course, name, max(score) from score  where course xxx group by course, name
  *
  *
  */
object _07SparkSQLWindowFuncOps3 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_07SparkSQLWindowFuncOps3")
            //            .enableHiveSupport()
            .getOrCreate()
        val scoreDF = spark.read.json("data/sql/topn.json")
        println("==================原始数据=========================")
        scoreDF.show()
        scoreDF.createOrReplaceTempView("score")
        println("==================每个科目成绩最大、最小的学生，科目平局值平均=========================")
        val sumHistorySQL1 =
            """
               |select
               |  distinct course,
               |  max(score) over(partition by course ) max_score,
               |  min(score) over(partition by course ) min_score,
               |  bround(avg(score) over(partition by course), 2) avg_score
               |from score
            """.stripMargin
        spark.sql(sumHistorySQL1).show()
        //求出各个科目的最大值 最小值 平局值
        val sumHistorySQL =
            """
              |select
              |  course,
              |  max(score) max_score,
              |  min(score) min_socre,
              |  avg(score) avg_score
              |from score
              |group by course
            """.stripMargin
//        spark.sql(sumHistorySQL).show()

        val retSQL =
            """
              |select
              |   tmp.course,
              |   s.name max_score_name,
              |   tmp.max_score,
              |   s1.name min_score_name,
              |   tmp.min_score,
              |   bround(tmp.avg_score, 2) avg_score
              |from (
              |  select
              |     course,
              |     max(score) max_score,
              |     min(score) min_score,
              |     avg(score) avg_score
              |  from score
              |  group by course
              |) tmp
              |left join score s on tmp.course = s.course
              |and tmp.max_score = s.score
              |left join score s1 on tmp.course = s1.course
              |and tmp.min_score = s1.score
            """.stripMargin

//        spark.sql(retSQL).show()
        spark.stop()
    }
}

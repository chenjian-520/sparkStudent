package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * SparkSQL中的开窗函数的使用：
  *     row_number()         --->分组topN(必须掌握)
  *     sum() over()         --->分组累加
  *     avg/min/max() over() --->分组求最大
  *
  */
object _05SparkSQLWindowFuncOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_04SparkSQLUDTFOps")
//            .enableHiveSupport()
            .getOrCreate()

        val topnDF = spark.read.json("data/sql/topn.json")
        topnDF.createOrReplaceTempView("stu_score")
        println("==================原始数据=========================")
        topnDF.show()
        println("===========计算各个科目学员成绩降序==================")
        val sql =
            """
              |select
              |    course,
              |    name,
              |    score,
              |    row_number() over(partition by course order by score desc) rank
              |from stu_score
            """.stripMargin
        spark.sql(sql).show()
        println("=========计算各个科目学员成绩降序Top3================")
//        val topnSQL =
//            """
//              |select
//              |    course,
//              |    name,
//              |    score,
//              |    row_number() over(partition by course order by score desc) rank
//              |from stu_score
//              |having rank < 4
//            """.stripMargin
        val topnSQL =
            """
              |select
              | tmp.*
              |from (
              |   select
              |      course,
              |      name,
              |      score,
              |      row_number() over(partition by course order by score desc) rank
              |   from stu_score
              |)tmp
              |where tmp.rank < 4
            """.stripMargin
        spark.sql(topnSQL).show
        spark.stop()
    }
}

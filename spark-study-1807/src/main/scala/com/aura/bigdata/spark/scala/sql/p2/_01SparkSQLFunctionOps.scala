package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 为什么要是用各种函数？什么是函数？sparksql包含哪些函数？
  *  函数分类：
  *     数学函数：abs(), tan(), floor()...
  *     日期函数：date(),date_add(),date_diff()
  *     字符串函数：len(),substr(),...
  *     统计函数： max(), sum(), avg()..
  *     特殊的函数：explode(),...
  *     开窗函数：row_number() over(), sum() over(), avg() over()
  *  把这些函数归纳起来，分为了三种：
  *     UDF(User definition Function 用户自定义函数)
  *         一路输入，一路输出
  *         abs(),year(),len()
  *     UDAF(User definition Aggregation Function 用户自定义聚合函数)
  *         多路输入，一路输出
  *         sum(), avg(),...,
  *     UDTF(User definition Table Function 用户自定义表函数)
  *         一路输入，多路输出
  *         explode()---->行转列(flatMap)
  */
object _01SparkSQLFunctionOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_01SparkSQLFunctionOps")
//            .enableHiveSupport()
            .getOrCreate()

        val topnDF = spark.read.json("data/sql/people.json")
        topnDF.show()
        //统计一：不同年龄的人数
        topnDF.createOrReplaceTempView("people")
        var sql =
            """
              |select
              |  age,
              |  count(1) countz
              |from people
              |group by age
              |order by countz
            """.stripMargin
        spark.sql(sql).show()
        //使用子查询来解决group by的字段是计算出来的问题

        sql =
            """
              |select
              | tmp.countz,
              | count(1) as new_count
              |from (
              | select
              |     age,
              |     count(age) countz
              | from people
              | group by age
              |) tmp
              |group by tmp.countz
              |order by new_count desc
            """.stripMargin

        spark.sql(sql).show()
        spark.stop()
    }
}

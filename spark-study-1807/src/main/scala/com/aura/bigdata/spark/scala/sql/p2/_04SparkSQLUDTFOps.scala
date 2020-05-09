package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * udtf操作，
  * 使用sparksql，来完成wordcount的统计(必会)
  */
object _04SparkSQLUDTFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_04SparkSQLUDTFOps")
            //            .enableHiveSupport()
            .getOrCreate()

        val helloDF = spark.read.text("data/hello.txt").toDF("line")
        println("---------------sparksql2wordcount----------------------")
        helloDF.show()

        helloDF.createOrReplaceTempView("tmp_hello")
        println("-----------------split拆分-------------------")
        var sql =
            """
              |select
              | split(line, '\\s+')
              |from tmp_hello
            """.stripMargin
        spark.sql(sql).show()
        println("-----------------explode压平-------------------")
        /*
            select xxx, count(x) from t group by xxx;
            |     [hello, me]|
            |    [hello, you]|
            |    [hello, you]
            hello
            me
            hello
            you
            hello
            you
         */
        sql =
            """
              |select
              | explode(split(line, '\\s+')) as word
              |from tmp_hello
            """.stripMargin
        spark.sql(sql).show()
        println("-----------------统计结果-------------------")
        sql =
            """
              |select
              | tmp.word,
              | count(tmp.word) as count
              |from (
              |  select
              |     explode(split(line, '\\s+')) as word
              |  from tmp_hello
              |) tmp
              |group by tmp.word
              |order by count desc
            """.stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
}

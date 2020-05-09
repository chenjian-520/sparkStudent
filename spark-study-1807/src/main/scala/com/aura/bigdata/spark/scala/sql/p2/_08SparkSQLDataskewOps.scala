package com.aura.bigdata.spark.scala.sql.p2

import java.util.Random

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * SparkSQL去处理DataSkew数据倾斜的问题：
  *
  * xxxByKey
  *     以wordcount为例去处理
  *     两阶段聚合（局部聚合+全局聚合） --->双重group by
  * join
  *
  */
object _08SparkSQLDataskewOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_08SparkSQLDataskewOps")
            .getOrCreate()
        /***********register start**********/
        spark.udf.register[String, String, Int]("addPrefix", addRandomPrefix)
        spark.udf.register[String, String]("removePrefix", removeRandomPrefix)
        /***********register end***********/
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
        sql =
            """
              |select
              | explode(split(line, '\\s+')) as word
              |from tmp_hello
            """.stripMargin
        spark.sql(sql).show()
        println("-----------------原始统计wordcount-------------------")
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
        println("--------------在上述原始统计wordcount过程中发生数据倾斜---------------")
        println(
            """解决因为xxByKey操作的数据倾斜
              |方案：两阶段聚合
              |1、找到发生数据倾斜的key
              |2、对发生数据倾斜的key做拆分（加上N以内的随机前缀）
              |3、做局部聚合
              |4、去掉随机前缀，然后做全局聚合
            """.stripMargin)

        val addPrefixSQL =
            """
              |select
              |  addPrefix(t1.word, 2) as r_p_word
              |from (
              | select
              |   explode(split(line, '\\s+')) word
              | from tmp_hello
              |) t1
            """.stripMargin
        spark.sql(addPrefixSQL).show()
        println("==============做局部聚合===================")
        val partAggrSQL =
            """
              |select
              |   t2.r_p_word,
              |   count(t2.r_p_word) as part_count
              |from (
              |	  select
              |	  addPrefix(t1.word, 2) as r_p_word
              |	  from (
              |	      select
              |	      explode(split(line, '\\s+')) word
              |	      from tmp_hello
              |	  ) t1
              |) t2
              |group by t2.r_p_word
            """.stripMargin
        spark.sql(partAggrSQL).show()

        println("==============去掉随机前缀===================")
        val removePrefixSQL =
            """
              |select
              |   removePrefix(t2.r_p_word) word,
              |   count(t2.r_p_word) as part_count
              |from (
              |	  select
              |	  addPrefix(t1.word, 2) as r_p_word
              |	  from (
              |	      select
              |	      explode(split(line, '\\s+')) word
              |	      from tmp_hello
              |	  ) t1
              |) t2
              |group by t2.r_p_word
            """.stripMargin
        spark.sql(removePrefixSQL).show()
        println("==============做全部聚合===================")
        val fullAggrSQL =
            """
              |select
              |   t3.word,
              |   sum(t3.part_count) count
              |from (
              |    select
              |       removePrefix(t2.r_p_word) word,
              |       count(t2.r_p_word) as part_count
              |    from (
              |    	  select
              |    	  addPrefix(t1.word, 2) as r_p_word
              |    	  from (
              |    	      select
              |    	      explode(split(line, '\\s+')) word
              |    	      from tmp_hello
              |    	  ) t1
              |    ) t2
              |    group by t2.r_p_word
              |) t3
              |group by t3.word
            """.stripMargin
        spark.sql(fullAggrSQL).show()
        spark.stop()
    }

    def addRandomPrefix(str:String, bound:Int) : String = {
        if(str == "hello") {
            val random = new Random()
            random.nextInt(bound) + "_" + str
        } else {
            str
        }
    }

    def removeRandomPrefix(str:String) : String = {
        str.substring(str.indexOf("_") + 1)
    }

}

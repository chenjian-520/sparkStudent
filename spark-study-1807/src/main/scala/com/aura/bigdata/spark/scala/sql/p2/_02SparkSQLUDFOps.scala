package com.aura.bigdata.spark.scala.sql.p2

import com.sun.rowset.internal.Row
import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * SparkSQL自定义UDF操作：
  *
  *  1、编写一个UDF函数，输入输出参数
  *  2、向SQLContext进行注册，该UDF
  *  3、就直接使用
  *
  *  案例：通过计算字符串长度的函数，来学习如何自定义UDF
  */
object _02SparkSQLUDFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_02SparkSQLUDFOps")
            //            .enableHiveSupport()
            .getOrCreate()
        //第二步：向SQLContext进行注册，该UDF
        spark.udf.register[Int, String]("strLen", str => strLen(str))

        val topnDF = spark.read.json("data/sql/people.json")
        topnDF.createOrReplaceTempView("people")
        topnDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[String]("age"),row.getAs[String]("height"))).foreach(row =>println(row))


        //自定义字符串长度函数，来就去表中name的长度
        //第三步：就直接使用
        val sql =
            """
              |select
              | name,
              | strLen(name) nameLen
              |from people
            """.stripMargin

        spark.sql(sql).show()
        spark.stop()
    }

    //第一步：编写一个UDF函数，输入输出参数
    def strLen(str:String):Int = str.length
}



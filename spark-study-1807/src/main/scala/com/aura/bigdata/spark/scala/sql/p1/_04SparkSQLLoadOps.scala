package com.aura.bigdata.spark.scala.sql.p1

import java.util.Properties

import com.aura.bigdata.spark.scala.sql.entity.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * sparkSQL中加载数据的方式
  *     json
  *     parquet
  *     text
  *     jdbc
  *    默认加载的parquet文件
  */
object _04SparkSQLLoadOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setAppName("_04SparkSQLLoadOps")
            .setMaster("local[2]")
        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()
        //sparkSQL加载外部的数据的方式
        val sqlContext = spark.sqlContext
//        sqlContext.load()//早起版本加载外部数据的方式，现在使用read
//        val topnDF = sqlContext.read.text("E:/data/spark/sql/topn.txt")//加载普通的文本文件
//        val topnDF = sqlContext.read.json("E:/data/spark/sql/sqldf.json")//加载json文件
        val pCSVDF = sqlContext.read.csv("data/persons.csv").toDF("name", "age", "gender")
        val url = "jdbc:mysql://localhost:3306/user_infos"
        val properties = new Properties()
        properties.put("user", "root")
        properties.put("password", "sorry")
        val jdbcDF = sqlContext.read.jdbc(url, "city", properties)
//        jdbcDF.show()
//        pCSVDF.show()
        sqlContext.read.format("csv").load("data/persons.csv").show()
        sqlContext.read.load("E:/data/spark/sql/sqldf.parquet").show()
        spark.stop()
    }
}

package com.aura.bigdata.spark.scala.sql.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 自定义UDAF操作:
  *  1、编写一个UDAF类，extends UserDefinedAggregateFunction
  *     复写其中的若干方法
  *  2、和UDF一样向SQLContext进行注册，该UDAF
  *  3、就直接使用
  *  模拟count函数
  *  可以参考在sparkCore中学习的combineByKey或者aggregateByKey
  */
object _03SparkSQlUDAFOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
            .master("local[2]")
            .appName("_02SparkSQLUDFOps")
            //            .enableHiveSupport()
            .getOrCreate()
        //2、和UDF一样向SQLContext进行注册，该UDAF
        spark.udf.register("myCount", new MyCountUDAF())

        val topnDF = spark.read.json("data/sql/people.json")
        topnDF.createOrReplaceTempView("people")
        topnDF.show()

        println("------------------------------")
        //3、就直接使用
        val sql =
            """
              |select
              |  age,
              |  myCount(age) countz
              |from people
              |group by age
            """.stripMargin
        spark.sql(sql).show()
        spark.stop()
    }
}

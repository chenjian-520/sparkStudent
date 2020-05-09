package com.aura.bigdata.spark.scala.sql.p1

import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}


/**
  * SparkSQL基本操作
  *
  * 在spark1.6以后引入了一个统一的编程入口模型：SparkSession
  *
  * SparkSession封装了SparkConf，SparkContext、SQLContext、HiveContext
  * 在2.x以前
  *     SQLContext、HiveContext同时存在，HiveContext extends SQLContext
  *   SQLContext支持标准sql，不支持Hive中的一些特殊语法，比如row_number()是不支持的。
  *   所以一般操作Hive的话，就是HiveContext
  *
  */
object _01SparkSQLOps {
    def main(args: Array[String]): Unit = {

        //step 1 创建程序的入口类SparkSession
        val spark = SparkSession.builder()
            .appName(s"${_01SparkSQLOps.getClass.getSimpleName}")
            .master("local[2]")
//            .enableHiveSupport()//这句话的意思，就是通过spark.sqlContext得到对象是HiveContext，反之就是SQLContext
            .getOrCreate()
        val sparkContext = spark.sparkContext

//        val sqlContext = new SQLContext(sparkContext)//在2.0之后主构造器被私有化，使用SparkSession
//        val hiveContext = new HiveContext(sparkContext)//在2.0之后主构造器被私有化，使用SparkSession.enableHiveSupport()

//        val sqlContext = spark.sqlContext
        //step 2加载数据
        val pDF:DataFrame = spark.read.json("E:/data/spark/sql/people.json") //这就是一张二维表

        //查看这张二维表结构
        pDF.printSchema()
        //查看这张二维表中的数据
        pDF.show()//默认查看当前二维表中的20条记录
        //对这张二维表进行复杂查询 只查询的姓名信息
        pDF.select("name").show()
        //只查询的姓名信息,年龄 同时年龄大于19的信息
        pDF.select(new Column("name"), new Column("age").>(19).as("是否大于19岁")).show()
        pDF.select("name", "age")//select name , age from person
           .where(new Column("age").>(19))
           .show()
        pDF.select("name", "age")//select name , age from person
            .where("age between 19 and 30")
            .show()
        //只查询的姓名信息,按照年龄进行分组 各年龄段人数select age, count(1) from t
        pDF.select("age")
            .groupBy("age").count().show()
        pDF.selectExpr("age")
                .groupBy("age")
                .count()
                .orderBy("count")
                .show()
        spark.stop()
    }
}

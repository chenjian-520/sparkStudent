package com.aura.bigdata.spark.scala.sql.p1

import org.apache.spark.sql.SparkSession

/**
  * 通过sparkSQL来完成对Hive的操作
  *     两张表：
  *         teacher_basic
  *             zhangsan,23,false,0
  *         teacher_info
  *  需求：
  *     1、利用sparksql，在hive中创建对应的两张表，teacher_basic,teacher_info,
  *     2、利用sparksql，加载对应的表数据
  *     3、利用sparksql，查询老师的所有的信息
  *     4、将结果落地到hive中的teacher表中
  * maven打包方式：mvn clean package -DskipTests
  *     maven的三大生命周期：Clean、Default、Site
  *Clean
  *     clean
  *Default
  *     complie（javac salac）
  *     test
  *     package（xxx.class--->jar）
  *     install（为了让本地的其它项目依赖，所以需要将当前package之后的jar包 安装到maven仓库中）
  *Site:站点信息
  *     deploy（为了让互联网或局域网内的其它用户使用该jar，需要将package之后的jar部署到对应的maven仓库，私服、镜像、中央仓库）
  *
  * 出现的问题：
  *     1、spark.sql()中没次只能执行一条sql，不能写多条sql，不然解析失败
  *         ser class threw exception: org.apache.spark.sql.catalyst.parser.ParseException:
            mismatched input ';' expecting <EOF>(line 2, pos 41)
            == SQL ==
            CREATE DATABASE IF NOT EXISTS `test_1807`;
            -----------------------------------------
        2、加载数据的语句写错
        3、加载数据的需要加载本地数据：
            file:///home/bigdata/data/spark/teacher_info.txt
        4、如果要和线上的hive进行整合需要做这么两件事：
            第一：将$HIVE_HOME/conf/hive-site.xml拷贝$SPARK_HOME/conf/下面
            第二：将mysql的jar，复制到$SPARK_HOME/jars/下面
            额外，如有有需要，同样将将$HIVE_HOME/conf/hive-site.xml可以拷贝到class_path下面
  *
  */
object _06SparkSQL2HiveOps {
    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val spark = SparkSession.builder()
                        .appName("SparkSQL2Hive")
                        .enableHiveSupport()
                        .getOrCreate()
        var sql =
            """
              |CREATE DATABASE IF NOT EXISTS `test_1807`
            """.stripMargin
        spark.sql(sql)//创建数据库
        sql =
            """
              |USE `test_1807`
            """.stripMargin
        spark.sql(sql)//使用当前创建的数据库
        sql =
            """
              |CREATE TABLE IF NOT EXISTS `test_1807`.`teacher_basic`(
              |   name string,
              |   age int,
              |   married boolean,
              |   children int
              |) row format delimited
              |fields terminated by ','
            """.stripMargin
        spark.sql(sql)//创建teacher_basic
        //teacher_info
        sql =
            """
              |CREATE TABLE IF NOT EXISTS `test_1807`.`teacher_info`(
              |   name string,
              |   height int
              |) row format delimited
              |fields terminated by ','
            """.stripMargin
        spark.sql(sql)

        //加载数据
        val loadInfoSQL =
            """
              |load data local inpath 'file:///home/bigdata/data/spark/teacher_info.txt' into table `test_1807`.`teacher_info`
            """.stripMargin
        val loadBasicSQL =
            """
              |load data local inpath 'file:///home/bigdata/data/spark/teacher_basic.txt' into table `test_1807`.`teacher_basic`
            """.stripMargin
        //加载数据
        spark.sql(loadInfoSQL)
        spark.sql(loadBasicSQL)
        //执行查询操作
        sql =
            """
              |select
              | i.name,
              | b.age,
              | b.married,
              | b.children,
              | i.height
              |from `test_1807`.`teacher_info` i
              |left join `test_1807`.`teacher_basic` b on i.name = b.name
            """.stripMargin
        val retDF = spark.sql(sql)

        retDF.write.saveAsTable("teacher")
        spark.stop()
    }
}

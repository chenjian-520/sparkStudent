package com.aura.bigdata.spark.scala.sql.p1

import java.util.Date

import com.aura.bigdata.spark.scala.sql.entity.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConversions

/**
  * SparkRDD和DataFrame/Datasets的转换：
  *  rdd--->dataframe/datasets
  *     1、通过反射的方式将RDD或者外部的集合转化为DataFrame或者Datasets
  *         如果用的dataframe，对应的java bean
  *         如果对应的datasets，对应的case class
  *         局限在于所有的在SparkSQL编程中涉及到的数据模型必须提前得设计好，不是很灵活
  *     2、要通过编程动态的来将外部集合或者RDD转化为DataFrame或者Datasets
  *  dataframe/datasets--->rdd
  *     dataframe/datasets.rdd就搞定
  */
object _03SparkRDD2DatasetsOps2 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setAppName("_02SparkRDD2DatasetsOps2")
            .setMaster("local[2]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[Person]))
        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()
        val sc = spark.sparkContext
        //创建dataframe或者dataset
        val list = List(
            new Person("委佩坤", "1998-11-11", "山西"),
            new Person("吴富豪", "1999-06-08", "河南"),
            new Person("戚长建", "2000-03-08", "山东"),
            new Person("王  伟", "1997-07-09", "安徽"),
            new Person("薛亚曦", "2002-08-09", "辽宁")
        )

        val personRDD:RDD[Person] = sc.parallelize(list)
        val personRowRDD:RDD[Row] = personRDD.map(person => {
            Row(person.getName, person.getBirthday, person.getProvince)
        })
        val schema = StructType(List(
            StructField("name", DataTypes.StringType, false),
            StructField("birthday", DataTypes.StringType, false),
            StructField("province", DataTypes.StringType, false)
        ))

        //list2DataFrame2(spark, list, schema)
        val personDF = spark.createDataFrame(personRowRDD, schema)

        //打印全量数据
        personDF.foreach(pRow => {
//            val name = pRow.getString(0)
            val name = pRow.getAs[String]("name")
            println("name: " + name)
        })

        val pRDD:RDD[Row] = personDF.rdd

        spark.stop()
    }

    private def list2DataFrame2(spark: SparkSession, list: List[Person], schema:StructType) = {
        val personRows = list.map(person => {
            Row(person.getName, person.getBirthday, person.getProvince)
        })
        val personRowList = JavaConversions.seqAsJavaList(personRows)

        val dynamicDF = spark.createDataFrame(personRowList, schema)
        //获取元数据信息
        dynamicDF.printSchema()
        //查询province中包含"山"的信息
        dynamicDF.select("name", "birthday", "province")
            .where("province like '山%'")
            .show()
    }
}

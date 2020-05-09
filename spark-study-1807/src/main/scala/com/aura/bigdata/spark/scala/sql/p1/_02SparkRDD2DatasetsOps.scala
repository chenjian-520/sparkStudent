package com.aura.bigdata.spark.scala.sql.p1

import java.util.Date

import com.aura.bigdata.spark.scala.sql.entity.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

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
object _02SparkRDD2DatasetsOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf  = new SparkConf()
                .setAppName("_02SparkRDD2DatasetsOps")
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
        val javaList = JavaConversions.seqAsJavaList(list)//JavaConversions java和scala中集合转化的工具类
        //rdd2DataFrame(spark, sc, list)
//        list2DataFrame(spark, javaList, classOf[Person])

        val stuList = List(
            new Student("委佩坤", "1998-11-11", "山西"),
            new Student("吴富豪", "1999-06-08", "河南"),
            new Student("戚长建", "2000-03-08", "山东"),
            new Student("王  伟", "1997-07-09", "安徽"),
            new Student("薛亚曦", "2002-08-09", "辽宁")
        )
        import spark.implicits._
        val persons = spark.createDataset(stuList)
//        persons.printSchema()
        persons.selectExpr("name", "province as p").show()
        spark.stop()
    }

    private def list2DataFrame(spark: SparkSession, javaList:java.util.List[_], beanClass:Class[_]): Unit = {
        val pDF = spark.createDataFrame(javaList, beanClass)
        val count = pDF.count()
        println("count: " + count)
    }
    private def rdd2DataFrame(spark: SparkSession, sc: SparkContext, list: List[Person]) = {
        val personRDD: RDD[Person] = sc.parallelize(list)
        //        personRDD.foreach(println)
        //使用反射的方式将一个RDD，转化为DataFrame
        val personDF: DataFrame = spark.createDataFrame(personRDD, classOf[Person])
        //        personDF.printSchema()
        //        personDF.show()
        //        personDF.registerTempTable()//2.0以前创建临时表的方式，
        personDF.createOrReplaceTempView("person")

        //        personDF.show()//查询personDF中的数据
        //使用sql查询province为山东或者山西
        val sql =
        """
          |select
          |   *
          |from person
          |where province like '山%'
        """.stripMargin
        spark.sql(sql).show()
    }
}

case class Student(name:String, birthday:String, province:String) {

}

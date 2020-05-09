package com.aura.bigdata.spark.scala.sql.p1

import java.util.Properties

import com.aura.bigdata.spark.scala.sql.entity.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * SparkSQL计算完成数据的落地
  * save的过程可能会报错，文件目录已经存在
  *     设置一个SaveMode
  *         ErrorIfExists   :如果目录存在，报错，默认选择
  *         Append          ：当前目录追加数据
  *         Ingore          ：忽略本次写入操作
  *         Overwrite       ：覆盖目录中的内容
  */
object _05SparkSQLWriteOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        val conf = new SparkConf()
            .setAppName("_05SparkSQLWriteOps")
            .setMaster("local[2]")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[Person]))
        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()
        val list = List(
            new Person("委佩坤", "1998-11-11", "山西"),
            new Person("吴富豪", "1999-06-08", "河南"),
            new Person("戚长建", "2000-03-08", "山东"),
            new Person("王  伟", "1997-07-09", "安徽"),
            new Person("薛亚曦", "2002-08-09", "辽宁")
        )
        val sc = spark.sparkContext
        val personRDD:RDD[Person] = sc.parallelize(list)
        val personRowRDD:RDD[Row] = personRDD.map(person => {
            Row(person.getName, person.getBirthday, person.getProvince)
        })
        val schema = StructType(List(
            StructField("name", DataTypes.StringType, false),
            StructField("birthday", DataTypes.StringType, false),
            StructField("province", DataTypes.StringType, false)
        ))

        val personDF = spark.createDataFrame(personRowRDD, schema)

        personDF.createTempView("table");
        personDF.select("*").show();


//        personDF.write.mode(SaveMode.Ignore).save("E:/data/spark/sql/out.txt")

//        personDF.write
//            .format("json")
//            .mode(SaveMode.Overwrite)
//            .save("E:/data/spark/sql/out.txt")
//        val url = "jdbc:mysql://localhost:3306/test"
//        val properties = new Properties()
//        properties.put("user", "root")
//        properties.put("password", "sorry")
//        personDF.write.mode(SaveMode.Append).jdbc(url, "person", properties)
        spark.stop()
    }
}

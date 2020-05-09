package com.aura.bigdata.spark.scala.core.p3

import com.aura.bigdata.spark.scala.core.p2._02SparkTransformationOps
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Spark的广播变量的操作
  *    有两张表学生表和班级表
  *    stu
  *     stu_id  stu_name    class_id
  *    class
  *     class_id    class_name
  *   将两张表数据合并，显示学生所有信息
  *     stu_id  stu_name   class_name
  *     join
  *
  */
object _02SparkRDDBroadcastOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_02SparkRDDBroadcastOps.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        val stu = List(
            "1  郑祥楷 1",
            "2  王佳豪 1",
            "3  刘鹰 2",
            "4  宋志华 3",
            "5  刘帆 4",
            "6  OLDLi 5"
        )

        val cls = List(
            "1 1807bd-bj",
            "2 1807bd-sz",
            "3 1807bd-wh",
            "4 1807bd-xa",
            "7 1805bd-bj"
        )
//        joinOps(sc, stu, cls)
        /*
            使用广播变量来完成上述操作
            一般用户表都比较大，而班级表相对很小，符合我们在共享变量中提出的第一个假设
            所以我们可以尝试使用广播变量来进行解决
         */
        val stuRDD = sc.parallelize(stu)
        //cls-->map---->
        val map = cls.map{case line => {
            (line.substring(0, line.indexOf(" ")), line.substring(line.indexOf(" ")).trim)
        }}.toMap
        //map--->broadcast
        val clsMapBC:Broadcast[Map[String, String]] = sc.broadcast(map)

        stuRDD.map{case line => {
            val map = clsMapBC.value
            val fields = line.split("\\s+")
            val cid = fields(2)
//            map.get(cid)
            val className = map.getOrElse(cid, "UnKnown")
            s"${fields(0)}\t${fields(1)}\t${className}"//在mr中学习到的map join
        }}.foreach(println)
        sc.stop()
    }

    private def joinOps(sc: SparkContext, stu: List[String], cls: List[String]) = {
        val stuRDD = sc.parallelize(stu)
        val clsRDD = sc.parallelize(cls)
        val cid2STURDD: RDD[(String, String)] = stuRDD.map { case line => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }
        }

        val cid2ClassRDD: RDD[(String, String)] = clsRDD.map { case line => {
            val fields = line.split("\\s+")
            (fields(0), fields(1))
        }
        }

        //两张表关联--join
        println("---------inner join-------------")//reduce join
        val cid2InfoRDD: RDD[(String, (String, String))] = cid2STURDD.join(cid2ClassRDD)
        cid2InfoRDD.foreach(println)
    }
}

package com.aura.bigdata.spark.scala.optimization

import java.sql.DriverManager

import com.aura.bigdata.spark.java.uitl.ConnectionPool
import com.aura.bigdata.spark.scala.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Spark开发调优案例
  * 使用高性能的算子：
  *     mapPartition
  */
object _01SparkDevelopOptimizationOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val sc = SparkUtil.sparkContext("local[2]", "_01SparkDevelopOptimizationOps")
//        mapPartitionOps(sc)
//        foreachOps(sc)
//        foreachPartitionOps(sc)
//        foreachPartitionOps2(sc)
        foreachPartitionOps3(sc)
        sc.stop
    }

    /**
      * 不能一次性都插入一个分区中的数据，比如没次插入5条记录
      * @param sc
      */
    private def foreachPartitionOps3(sc: SparkContext): Unit = {
        val lines = sc.textFile("data/test.txt")
        val words = lines.flatMap(_.split("\\s+"))
        val accu = sc.longAccumulator("partitions")
        val pairs = words.mapPartitions{case words => {
            accu.add(1)
            val ab = ArrayBuffer[(String, Int)]()
            words.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }}

        val rbkRDD:RDD[(String, Int)] = pairs.reduceByKey(_+_, 1)

        rbkRDD.foreachPartition(pIterator => {
            val connection = ConnectionPool.getConnection

            val sql =
                """INSERT INTO wordcount
                  |VALUES(?, ?)
                """.stripMargin
            val ps = connection.prepareStatement(sql)
            var size = 0
//            println("length: " + pIterator.length)
            pIterator.foreach{case (word, count) => {
                size += 1
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch()//加入批量操作中
                if(size >= 5) {//提交
                    ps.executeBatch()
                    size = 0
                }
            }}

           //提交哪些在foreach中没有被提交的数据
            if(size != 0) {
                println("size: " + size)
                ps.executeBatch()
            }
            ps.close()
            ConnectionPool.returnConnection(connection)
        })
        println("partitions: " + accu.value)
    }


    private def foreachPartitionOps2(sc: SparkContext): Unit = {
        val lines = sc.textFile("data/test.txt")
        val words = lines.flatMap(_.split("\\s+"))
        val accu = sc.longAccumulator("partitions")
        val pairs = words.mapPartitions{case words => {
            accu.add(1)
            val ab = ArrayBuffer[(String, Int)]()
            words.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }}

        val rbkRDD:RDD[(String, Int)] = pairs.reduceByKey(_+_)

        rbkRDD.foreachPartition(pIterator => {
            val connection = ConnectionPool.getConnection

            val sql =
                """INSERT INTO wordcount
                  |VALUES(?, ?)
                """.stripMargin
            val ps = connection.prepareStatement(sql)
            pIterator.foreach{case (word, count) => {
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch()//加入批量操作中
            }}
            ps.executeBatch()//批量提交

            ps.close()
            ConnectionPool.returnConnection(connection)
        })
        println("partitions: " + accu.value)
    }

    private def foreachPartitionOps(sc: SparkContext): Unit = {
        val lines = sc.textFile("data/test.txt")
        val words = lines.flatMap(_.split("\\s+"))
        val accu = sc.longAccumulator("partitions")
        val pairs = words.mapPartitions{case words => {
            accu.add(1)
            val ab = ArrayBuffer[(String, Int)]()
            words.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }}

        val rbkRDD:RDD[(String, Int)] = pairs.reduceByKey(_+_)

        rbkRDD.foreachPartition(pIterator => {
            //加载驱动 step 1
            classOf[com.mysql.jdbc.Driver]
            //连接 step 2
            val connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/test",
                "root",
                "sorry"
            )
            // step 3 statement
            val sql =
                """INSERT INTO wordcount
                  |VALUES(?, ?)
                """.stripMargin
            val ps = connection.prepareStatement(sql)
            pIterator.foreach{case (word, count) => {
                ps.setString(1, word)
                ps.setInt(2, count)
                ps.addBatch()//加入批量操作中
            }}
            ps.executeBatch()//批量提交

            ps.close()
            connection.close()
        })
        println("partitions: " + accu.value)
    }

    /**
      * 使用foreach操作将结果落地到MySQL
      * 计算data/test.txt文件中每个单词出现的次数，并将结果落地到MySQL
      * CREATE TABLE wordcount (
            word VARCHAR(20),
            `count` INT
        );
      * @param sc
      */
    private def foreachOps(sc: SparkContext): Unit = {
        val lines = sc.textFile("data/test.txt")
        val words = lines.flatMap(_.split("\\s+"))
        val pairs = words.mapPartitions{case words => {
            val ab = ArrayBuffer[(String, Int)]()
            words.foreach(word => {
                ab.append((word, 1))
            })
            ab.iterator
        }}

        val rbkRDD:RDD[(String, Int)] = pairs.reduceByKey(_+_)


        rbkRDD.foreach{case (word, count) => {
            //加载驱动 step 1
            classOf[com.mysql.jdbc.Driver]
            //连接 step 2
            val connection = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/test",
                "root",
                "sorry"
            )
            // step 3 statement
            val sql =
                """INSERT INTO wordcount
                  |VALUES(?, ?)
                """.stripMargin
            val ps = connection.prepareStatement(sql)
            ps.setString(1, word)
            ps.setInt(2, count)
            ps.execute()

            ps.close()
            connection.close()
        }}
    }


    private def mapPartitionOps(sc: SparkContext) = {
        val lines = sc.textFile("data/topn.txt")
        val partitions = lines.partitions

        partitions.foreach(p => {
            println(p.index)
        })

        val className2InfoRDD = lines.mapPartitions(pIterator => {
            val ab = ArrayBuffer[(String, (String, Int))]()
            pIterator.foreach(line => {
                val fields = line.split("\\s+")
                val className = fields(0)
                val name = fields(1)
                val score = fields(2).toInt
                ab.append((className, (name, score)))
            })
            ab.iterator
        })

        className2InfoRDD.count()
    }
}

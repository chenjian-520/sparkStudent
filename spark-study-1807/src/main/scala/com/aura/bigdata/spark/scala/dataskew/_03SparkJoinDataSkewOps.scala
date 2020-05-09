package com.aura.bigdata.spark.scala.dataskew

import java.util.Random

import com.aura.bigdata.spark.scala.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * 采样倾斜key并分拆join操作
  *  两张表t1 和 t2进行join
  *     t1(id, log)
  *     t2(id, name)
  *
  */
object _03SparkJoinDataSkewOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val sc = SparkUtil.sparkContext("local[2]", "_03SparkJoinDataSkewOps")
        val list1 = List(
            "hello 1",
            "hello 2",
            "hello 3",
            "hello 4",
            "you 1",
            "me 2"
        )

        val list2 = List(
            "hello zhangsan dfsadfasdfsa",
            "hello lisi adfasdfasd",
            "you wangwu adfasdfs",
            "me zhouqi adfadfa"
        )
        //<key, value>
        val listRDD1 = sc.parallelize(list1).map(line => {
            val fields = line.split("\\s+")
            (fields(0), fields(1))
        })
        //<key, value>
        val listRDD2 = sc.parallelize(list2).map(line => {
            val fields = line.split("\\s+")
            (fields(0), fields(1))
        })
        val joinRDD: RDD[(String, (String, String))] = dataSkewRDDJoin(sc, listRDD1, listRDD2)
        println("最后进行join的结果：")
        joinRDD.foreach(println)
        sc.stop()
    }

    private def dataSkewRDDJoin(sc: SparkContext, listRDD1: RDD[(String, String)], listRDD2: RDD[(String, String)]) = {
        //假设listRDD1中的部分key有数据倾斜，所以我在进行join操作的时候，需要进行拆分计算
        //step 1 找到发生数据倾斜的key
        val dataSkewKeys = listRDD1.sample(false, 0.6).countByKey().toList.sortWith((t1, t2) => t1._2 > t2._2).take(1).map(t => t._1)
        println("通过sample算子得到的可能发生数据倾斜的key：" + dataSkewKeys)
        //step 2 对listRDD1和listRDD2中的数据按照dataSkewKeys各拆分成两个部分
        //step 2.1 讲dataSkewKeys进行广播
        val dskBC = sc.broadcast(dataSkewKeys)
        // step 2.2 进行拆分
        val dataSkewRDD1 = listRDD1.filter { case (word, value) => {
            //有数据倾斜的rdd--->dataskewRDD1
            val dsks = dskBC.value
            dsks.contains(word)
        }
        }

        val commonRDD1 = listRDD1.filter { case (word, value) => {
            //没有数据倾斜的rdd--->commonRDD1
            val dsks = dskBC.value
            !dsks.contains(word)
        }
        }
        val dataSkewRDD2 = listRDD2.filter { case (word, value) => {
            //有数据倾斜的rdd--->dataskewRDD1
            val dsks = dskBC.value
            dsks.contains(word)
        }
        }

        val commonRDD2 = listRDD2.filter { case (word, value) => {
            //没有数据倾斜的rdd--->commonRDD1
            val dsks = dskBC.value
            !dsks.contains(word)
        }
        }
        println("---->有数据倾斜RDD1拆分成dataskewRDD的结果：" + dataSkewRDD1.collect().mkString(","))
        println("---->无数据倾斜RDD1拆分成commonRDD的结果：" + commonRDD1.collect().mkString(","))
        println("---->有数据倾斜RDD2拆分成dataskewRDD的结果：" + dataSkewRDD2.collect().mkString(","))
        println("---->无数据倾斜RDD2拆分成commonRDD的结果：" + commonRDD2.collect().mkString(","))
        //step 3 对dataskewRDD进行添加N以内随机前缀
        // step 3.1 添加随机前缀
        val prefixDSRDD1:RDD[(String, String)] = dataSkewRDD1.map { case (word, value) => {
            val random = new Random()
            val prefix = random.nextInt(2)
            (s"${prefix}_${word}", value)
        }
        }
        // step 3.2 另一个rdd进行扩容
        val prefixDSRDD2:RDD[(String, String)] = dataSkewRDD2.flatMap { case (word, value) => {
            val ab = ArrayBuffer[(String, String)]()
            for (i <- 0 until 2) {
                ab.append((s"${i}_${word}", value))
            }
            ab
        }
        }
        println("---->有数据倾斜RDD1添加前缀成prefixDSRDD1的结果：" + prefixDSRDD1.collect().mkString(","))
        println("---->有数据倾斜RDD2扩容之后成prefixDSRDD2的结果：" + prefixDSRDD2.collect().mkString(","))

        // step 4 分步进行join操作
        // step 4.1 有数据倾斜的prefixDSRDD1和prefixDSRDD2进行join
        val prefixJoinDSRDD = prefixDSRDD1.join(prefixDSRDD2)
        //ste 4.2 无数据倾斜的commonRDD1和commonRDD2进行join
        val commonJoinRDD = commonRDD1.join(commonRDD2)
        println("---->有数据倾斜join之后的结果：" + prefixJoinDSRDD.collect().mkString(","))
        println("---->无数据倾斜join之后的结果：" + commonJoinRDD.collect().mkString(","))
        // step 4.3 将随机前缀去除
        val dsJionRDD = prefixJoinDSRDD.map { case (word, (value1, value2)) => {
            (word.substring(2), (value1, value2))
        }
        }
        //step 5 将拆分进行join之后的结果进行union连接，得到最后的结果 sql union all
        val joinRDD = dsJionRDD.union(commonJoinRDD)
        joinRDD
    }
}

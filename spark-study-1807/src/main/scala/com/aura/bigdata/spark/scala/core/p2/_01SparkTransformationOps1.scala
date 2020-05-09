package com.aura.bigdata.spark.scala.core.p2

import org.apache.avro.TestAnnotation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * 主要学习SparkTransformation的操作算子
  * 1、map：将集合中每个元素乘以7
  * 2、filter：过滤出集合中的奇数
  * 3、flatMap：将行拆分为单词
  * 4、sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
  * 5、union：返回一个新的数据集，由原数据集和参数联合而成
  * 6、groupByKey：对数组进行 group by key操作 慎用
  * 7、reduceByKey：统计每个班级的人数
  * 8、join：打印关联的组合信息
  * 9、sortByKey：将学生身高进行排序
  * 10、combineByKey
  * 11、aggregateByKey
  */
object _01SparkTransformationOps1 {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${_01SparkTransformationOps1.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        val list = List(1, 2, 3, 4, 5, 6, 7)
//        transformationMap_01(list, sc)
//        transformationFlatMap_02(sc)
//        transformationFilter_03(list, sc)
//        transformationSample_04(sc)
//        transformationUnion_05(sc)
//        transformationGBK_06(sc)
//        transformationRBK_07(sc)
//        transformationJOIN_08(sc)
        transformationUnion_05(sc)
        sc.stop()
    }

    /**
      * sortByKey：将学生成绩进行排序
      *     分区内部有序
      * 学生表stu:id   name    chinese-score
      *
      */
    def transformationsbk_09(sc:SparkContext): Unit = {
        val stuList = List(
            "1  王浩玉 93.5",
            "2  贾小红 56.5",
            "3  薛亚曦 60.5",
            "4  宋其 125"
        )
        val stuRDD = sc.parallelize(stuList)
        println("------------------sortByKey----------------------")
        val score2InfoRDD:RDD[(Double, String)] = stuRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2).toDouble, line)
        }}
        score2InfoRDD.sortByKey(false, numPartitions = 1).foreach(println)
        println("------------------sortBy----------------------")
        val sbkRDD2:RDD[String] = stuRDD.sortBy(line => line, false, 1)(
            new Ordering[String](){//按照成绩排序
                override def compare(x: String, y: String) = {
                    val xScore = x.split("\\s+")(2).toDouble
                    val yScore = y.split("\\s+")(2).toDouble
                    yScore.compareTo(xScore)
                }
            },
            ClassTag.Object.asInstanceOf[ClassTag[String]]
        )
        sbkRDD2.foreach(println)
    }

    /**
      * join: 打印关联的组合信息
      *    关联操作中rdd的类型必须(K, V)
      *    inner join:等值连接  返回左右两张表中能对应上的数据
      *    left/right outer join：左(右)外连接--->
      *         返回左(右)表所有数据，右(左)表能对应上的话显示，对应不上的话显示为null
      *    left/right full outer join
      *         返回左右两张表中都有的数据：左外连接+右外连接
      * 一张学生信息表stu，一张班级信息表class
      *     stu--->  stu_id  stu_name    cid
      *     class--> cid    cname
      * 现在要求查询以下信息：
      *     stu_id stu_name cname
      *  用SQL：
      *     select
      *         s.stu_id,
      *         s.stu_name,
      *         c.cname
      *     from stu s
      *     left join class c on s.cid = c.cid
      * @param sc
      */
    def  transformationJOIN_08(sc:SparkContext): Unit = {
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
        val stuRDD = sc.parallelize(stu)
        val clsRDD = sc.parallelize(cls)
        val cid2STURDD:RDD[(String, String)] = stuRDD.map{case line => {
            val fields = line.split("\\s+")
            (fields(2), line)
        }}
        val cid2ClassRDD:RDD[(String, String)] = clsRDD.map{case line => {
            val fields = line.split("\\s+")
            (fields(0), fields(1))
        }}
        //两张表关联--join
        println("---------inner join-------------")
        val cid2InfoRDD:RDD[(String, (String, String))] = cid2STURDD.join(cid2ClassRDD)
        cid2InfoRDD.foreach(println)
        println("---------left join-------------")
        val cid2LefJoinRDD:RDD[(String, (String, Option[String]))] = cid2STURDD.leftOuterJoin(cid2ClassRDD)
        cid2LefJoinRDD.foreach(println)
        println("---------full join-------------")
        val cid2FullJoinRDD:RDD[(String, (Option[String], Option[String]))] = cid2STURDD.fullOuterJoin(cid2ClassRDD)
        cid2FullJoinRDD.foreach(println)
    }
    /**
      *
      * 一张表，student表
      *     stu_id  stu_name   class_id
      *  统计每个班级的人数
      *  相同的统计下，reduceByKey要比groupByKey效率高，因为在map操作完毕之后发到reducer之前
      *  需要先进行一次本地的预聚合，每一个mapper(对应的partition)执行一次预聚合
      * @param sc
      */
    def  transformationRBK_07(sc:SparkContext): Unit = {
        val list = List(
            "1  郑祥楷 1807bd-bj",
            "2  王佳豪 1807bd-bj",
            "3  刘鹰 1807bd-sz",
            "4  宋志华 1807bd-wh",
            "5  刘帆 1807bd-xa",
            "6  何昱 1807bd-xa"
        )
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2Count = listRDD.map { case (line) => {
            val fields = line.split("\\s+")
            (fields(2), 1)
        }}
        cid2Count.reduceByKey(_+_).foreach(println)
    }
    /**
      * groupByKey：对数组进行 group by key操作 慎用
      * 一张表，student表
      *     stu_id  stu_name   class_id
      * 将学生按照班级进行分组，把每一个班级的学生整合到一起
      * 建议groupBykey在实践开发过程中，能不用就不用，主要是因为groupByKey的效率低，
      * 因为有大量的数据在网络中传输，而且还没有进行本地的预处理
      * 我可以使用reduceByKey或者aggregateByKey或者combineByKey去代替这个groupByKey
      */
    def  transformationGBK_06(sc:SparkContext): Unit = {
        val list = List(
            "1  郑祥楷 1807bd-bj",
            "2  王佳豪 1807bd-bj",
            "3  刘鹰 1807bd-sz",
            "4  宋志华 1807bd-wh",
            "5  刘帆 1807bd-xa",
            "6  何昱 1807bd-xa"
        )
        val listRDD = sc.parallelize(list)
        println("分区个数：" + listRDD.getNumPartitions)
        val cid2StuInfo = listRDD.map { case (line) => {
            val fields = line.split("\\s+")

            (fields(2), line)
        }}
        val gbkRDD = cid2StuInfo.groupByKey()
        gbkRDD.foreach{case (cid, stus) => {
            println(s"$cid ---> ${stus} ----> ${stus.size}")
        }}
    }
    /**
      * union：返回一个新的数据集，由原数据集和参数联合而成
      *   该union操作和sql中的union all操作一模一样
      */
    def  transformationUnion_05(sc:SparkContext): Unit = {
        val list1 = List(1, 2, 3, 4, 5)
        val list2 = List(6, 5, 4, 7, 9)
        val lRDD1 = sc.parallelize(list1)
        val lRDD2 = sc.parallelize(list2)

        lRDD1.union(lRDD2).foreach(t => print(t + " "))
    }

    /**
      * sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
      * 参数：
      *     withReplacement：true或者false
      *         true：代表有放回的抽样
      *         false：代表无放回的抽样
      *     fraction：抽取样本空间占总体的比例(分数的形式传入)
      *         without replacement： 0 <= fraction <= 1
      *         with replacement: fraction >= 0
      *     seed:随机数生成器
      *     new Random().nextInt(10)
      *     注意：我们使用sample算子不能保证提供集合大小就恰巧是rdd.size * fraction,
      * 结果大小会在前面数字上下浮动
      *   sample算子，在我们后面学习spark调优(dataskew)的时候，可以用的到
      */
    def transformationSample_04(sc:SparkContext): Unit = {
        val list = 0 to 99999
        val listRDD = sc.parallelize(list)
        val sampleRDD = listRDD.sample(false, 0.05)
        println(s"sampleRDD的集合大小：${sampleRDD.count()}")
    }

    /**
      * filter：过滤出集合中的奇数(even)
      */
    def transformationFilter_03(list:List[Int], sc:SparkContext): Unit = {
        val listRDD = sc.parallelize(list    )
        val filterRDD = listRDD.filter(num => {
//            1 / 0
            num % 2 == 0
        })
        filterRDD.foreach(println)
    }

    /**
      * 3、flatMap：将行拆分为单词
      *     和map算子类似，只不过呢，rdd形态转化对应为one-to-many
      * @param sc
      */
    def transformationFlatMap_02(sc:SparkContext): Unit = {
        val list = List(
            "lu jia hui",
            "chen zhi xiang"
        )
        val listRDD = sc.parallelize(list)
        listRDD.flatMap(line => line.split("\\s+")).foreach(println)
    }
    /**
      * 1、map：将集合中每个元素乘以7
      *   map是最常用的转换算子之一，将原rdd的形态，转化为另外一种形态，
      *   需要注意的是这种转换是one-to-one
      * @param sc
      */
    def transformationMap_01(list:List[Int], sc:SparkContext): Unit = {
        val bs = 7
        val listRDD = sc.parallelize(list)
        val retRDD = listRDD.map(num => num * bs)
        retRDD.foreach(println)
    }
}

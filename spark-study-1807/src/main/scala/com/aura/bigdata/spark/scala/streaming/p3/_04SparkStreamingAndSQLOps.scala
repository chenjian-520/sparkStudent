package com.aura.bigdata.spark.scala.streaming.p3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark最强大的地方在于，可以与Spark Core、Spark SQL整合使用，之前已经通过transform、foreachRDD等算子看到，
  * 如何将DStream中的RDD使用Spark Core执行批处理操作。现在就来看看，如何将DStream中的RDD与Spark SQL结合起来使用。
  *
  *
  *top3的商品排序： 最新的top3
  *     id  brand   category
        001 mi moblie
        002 mi moblie
        003 mi moblie
        004 mi moblie
        005 huawei moblie
        006 huawei moblie
    实时统计，截止到目前为止产生的不同品类下面的品牌销量的top3
  *
  * group by category, brand
  * row_number() 开窗函数
  * updateStateByKey--->截止到目前
  *
  * df.createOrReplaceGlobalTempView()  ---->在当前Application中有效
    df.createOrReplaceTempView()        ---->在当前SparkSession中有效
  */
object _04SparkStreamingAndSQLOps {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.project-spark").setLevel(Level.WARN)

        if(args == null || args.length < 4) {
            println(
                """Parameter Errors! Usage: <host> <port> <batchInterval> <checkpoint>
                  |host         :   连接主机名
                  |port         :   连接端口
                  |batchInterval:   批次提交间隔时间
                  |checkpoint   :   checkpoint目录
                """.stripMargin)
            System.exit(-1)
        }

        val Array(host, port, batchInterval, checkpoint) = args
        val conf = new SparkConf().setMaster("local[2]").setAppName("_04SparkStreamingAndSQLOps")
        val spark = SparkSession.builder()
                            .config(conf)
                            .getOrCreate()
        val batchDuration = Seconds(batchInterval.toLong)
        val ssc = new StreamingContext(spark.sparkContext, batchDuration)
        ssc.checkpoint(checkpoint)

        //1、加载外部数据
        val lines = ssc.socketTextStream(host, port.toInt)
//        001 mi moblie
        val pairs = lines.map(line => {
            val fields = line.split("\\s+")
            val key = fields(1) + "_" + fields(2)
            (key, 1)
        })
        //2、状态统计    --->checkpoint
        val usbDStream:DStream[(String, Int)] = pairs.updateStateByKey[Int]((seq:Seq[Int], option:Option[Int]) => {
            Option[Int](seq.sum + option.getOrElse(0))
        })
        //3、求取不同品类下面的品牌销量topN
       usbDStream.foreachRDD(rdd => {
           if(!rdd.isEmpty()) {
               val rowRDD:RDD[Row] = rdd.map{case (fhkey, count) => {
                   val keys = fhkey.split("_")
                   Row(keys(0), keys(1), count)//brand category count
               }}
               val schema = StructType(List(
                   StructField("brand", DataTypes.StringType, false),
                   StructField("category", DataTypes.StringType, false),
                   StructField("sales", DataTypes.IntegerType, false)
               ))
               val df = spark.createDataFrame(rowRDD, schema)
               df.createOrReplaceTempView("product_sale_view_tmp")
               val sql =
                   """
                     |select
                     |  tmp.category,
                     |  tmp.brand,
                     |  tmp.sales,
                     |  tmp.rank
                     |from (
                     |  select
                     |      category,
                     |      brand,
                     |      sales,
                     |      row_number() over(partition by category order by sales desc) as rank
                     |  from product_sale_view_tmp
                     |) tmp
                     |where tmp.rank < 4
                   """.stripMargin
               spark.sql(sql).show()
           }
       })


        ssc.start()
        ssc.awaitTermination()
    }
}

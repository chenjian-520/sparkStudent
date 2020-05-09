package com.aura.bigdata.spark.scala.sql.p2

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
  * 自定义UDAF
  */
class MyCountUDAF extends UserDefinedAggregateFunction {
    //该udaf的输入的数据类型
    override def inputSchema: StructType = {
        StructType(List(
            StructField("age", DataTypes.IntegerType, false)
        ))
    }

    /**
      * 在该udaf聚合过程中的数据的类型Schema
      */
    override def bufferSchema: StructType = {
        StructType(List(
            StructField("age", DataTypes.IntegerType, false)
        ))
    }

    //该udaf的输出的数据类型
    override def dataType: DataType = DataTypes.IntegerType

    //确定性判断，通常特定输入和输出的类型一致
    override def deterministic: Boolean = true
    /**
        初始化的操作
      var sum = 1
      for(i <- 0 to 9) {
        sum += i
      }
      row.get(0)
      @param buffer 就是我们计算过程中的临时的存储了聚合结果的Buffer(extends Row)
     */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, 0)//更新当前buffer数组中的第1列（索引为0）的值为0
    }

    /**
      * 分区内的数据聚合合并
      * @param buffer 就是我们在initialize方法中声明初始化的临时缓冲区
      * @param input  聚合操作新传入的值
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val oldValue = buffer.getInt(0) //row.get(0)
        buffer.update(0,  oldValue + 1)
    }

    /**
      * 分区间的数据聚合合并
      * 聚合之后将结果传递给分区一
      * @param buffer1 分区一聚合的临时结果
      * @param buffer2 分区二聚合的临时结果
      *                reduce(v1, v2)
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val pav1 = buffer1.getInt(0)
        val pav2 = buffer2.getInt(0)
        buffer1.update(0, pav1 + pav2)
    }

    /**
      * 该聚合函数最终要返回的值
      * @param buffer 数据就被存储在该buffer中
      */
    override def evaluate(buffer: Row): Any = {
        buffer.getInt(0)
    }
}

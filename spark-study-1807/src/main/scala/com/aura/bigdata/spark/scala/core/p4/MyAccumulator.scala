package com.aura.bigdata.spark.scala.core.p4

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 该自定义累加器的输入参数的类型：String
  * 输出：mutable.Map[String, Int]
  */
class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {
    private var res: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()

    //当前累加器是否有初始化值
    override def isZero: Boolean = res.isEmpty

    //累加器之间的复制
    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
        val myAccu = new MyAccumulator
        myAccu.res = this.res
        myAccu
    }
    //累加器的值得重置：清空
    override def reset(): Unit = {
        res.clear()
    }

    /**
      * 累加器累加字段
      * @param field
      */
    override def add(field: String): Unit = {
        val oldV = res.getOrElse(field, 0)
        res.put(field, oldV + 1)
    }

    /**
      * 分布式并行操作，所以最后做累加器之间的合并
      * @param other
      */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
        val oMap = other.value
        if(!oMap.isEmpty) {
            oMap.foreach{case (field, value) => {
                this.res.put(field, this.res.getOrElse(field, 0) + value)
            }}
        }
    }

    override def value: mutable.Map[String, Int] = this.res
}

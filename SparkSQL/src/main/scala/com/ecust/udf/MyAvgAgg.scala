package com.ecust.udf

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * @author Jinxin Li
 * @create 2021-01-02 11:29
 * 自定义聚合函数类,主要原则是进行计算
 * sql文是没有类型的
 * 这种弱类型聚合函数不支持
 */

/*
自定义聚合函数类,计算年龄的平均值
1.继承org.apache.spark.sql.expressions.Aggregator[]
2.定义泛型
  IN:输入的数据类型 Long
  BUF:缓冲区的数据类型 Buff 可以使用样例类方便调用
  OUT:输出的数据类型 Long
3.重写方法(6)
 */
case class Buff(var total:Long,var count:Long)
class MyAvgAgg extends Aggregator[Long,Buff,Long]{

  //scala这个z打头,都是零值,或者初始值
  override def zero: Buff = {
    //缓冲区的初始化
    Buff(0,0)
  }

  //根据输入的数据来更新缓冲区的数据
  override def reduce(b: Buff, a: Long): Buff = {
    val total: Long = b.total + a
    val count: Long = b.count+1
    Buff(total, count)
  }

  //合并缓冲区
  override def merge(b1: Buff, b2: Buff): Buff = {
    val total: Long = b1.total+b2.total
    val count: Long = b1.count+b2.count
    Buff(total, count)
  }

  override def finish(reduction: Buff): Long = reduction.total/reduction.count

  override def bufferEncoder: Encoder[Buff] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

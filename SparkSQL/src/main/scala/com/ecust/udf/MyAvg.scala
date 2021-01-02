package com.ecust.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

/**
 * @author Jinxin Li
 * @create 2021-01-02 11:29
 * 自定义聚合函数类,主要原则是进行计算
 * sql文是没有类型的
 * 这种弱类型聚合函数不支持
 */
class MyAvg extends UserDefinedAggregateFunction{

  //数据输入结构
  override def inputSchema: StructType = {
    StructType(Array(StructField("age",LongType)))
  }

  //缓冲区做计算的数据结构
  override def bufferSchema: StructType = {
    StructType(Array(StructField("total",LongType),StructField("count",LongType)))
  }

  //返回值类型
  override def dataType: DataType = DoubleType

  //稳定性
  override def deterministic: Boolean = true

  //缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L//sum
    buffer(1)=0L//count
  }

  //更新缓冲区中的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
//    buffer.update
  }

  //合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  //做最后的计算
  override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble/buffer.getLong(1).toDouble
  }
}

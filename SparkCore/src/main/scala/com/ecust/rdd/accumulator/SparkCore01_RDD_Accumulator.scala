package com.ecust.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-26 11:42
 */
object SparkCore01_RDD_Accumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //行动算子返回非RDD
    val sumRDD: Int = rdd.reduce(_ + _)//分区间计算,分区内计算

    println(sumRDD)
    sc.stop()

  }
}

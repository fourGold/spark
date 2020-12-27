package com.ecust.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-26 11:42
 */
object SparkCore03_RDD_Accumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),1)

    //todo 自定义累加器
    val sum: LongAccumulator = sc.longAccumulator("sum")
    //系统自带了一些累加器
//    sc.doubleAccumulator
//    sc.collectionAccumulator()

    rdd.foreach(num=>sum.add(num))

    println("driver:"+sum.value)
    sc.stop()
  }
}

package com.ecust.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Jinxin Li
 * @create 2020-12-27 13:34
 */
object SparkCore04_RDD_BroadCast {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //定义广播变量
    val value: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    //每个task都有一份数据
    val result: RDD[(String, (Int, Int))] = rdd1.map { case (w, c) => {
      val i: Int = value.value.getOrElse(w, 0)
      (w, (c, i))
    }}
    result.collect().foreach(println)
    sc.stop()
  }
}

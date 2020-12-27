package com.ecust.rdd.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-27 13:34
 */
object SparkCore01_RDD_BroadCast {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("acc")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    //rdd之间的join会导致rdd之间的几何倍数增长,不是很建议使用,并且会影响shuffle性能,不推荐使用
    //val joinRDD:RDD[(String,(Int,Int))] = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)
    //(a,1)(b,2)(c,3)
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)//(a,(1,4)) (b,(2,5)) (c,(3,6))

    joinRDD.collect().foreach(println)
    sc.stop()
  }
}

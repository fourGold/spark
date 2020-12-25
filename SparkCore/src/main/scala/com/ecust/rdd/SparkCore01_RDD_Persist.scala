package com.ecust.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 */
object SparkCore01_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    //生成RDD
    val listRDD: RDD[String] = sc.makeRDD(list, 1)
    val wordRDD: RDD[String] = listRDD.flatMap(word => word.split(" "))
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    val resultRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    resultRDD.collect().foreach(println)
    println("测试版本0.2")
    println("测试版本0.3-hotfix")
    sc.stop()
  }
}

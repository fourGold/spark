package com.ecust.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-30 19:04
 */
object SparkCore01_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val source: RDD[String] = sc.makeRDD(List("hello word", "hello spark"))

    val word = source.flatMap(_.split(" "))
    val wordToOne = word.map((_, 1))

    val result= wordToOne.reduceByKey(_ + _)

    result.collect().foreach(println)

    sc.stop()
  }
}

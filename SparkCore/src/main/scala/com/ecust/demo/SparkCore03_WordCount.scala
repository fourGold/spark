package com.ecust.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-30 19:04
 */
object SparkCore03_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val source: RDD[String] = sc.makeRDD(List("hello word", "hello spark"))

    val word = source.flatMap(_.split(" "))

    // todo group by 按照当前元素进行分组
    val group: RDD[(String, Iterable[String])] = word.groupBy(word => word)

    val result: RDD[(String, Int)] = group.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    result.collect().foreach(println)

    sc.stop()
  }
}

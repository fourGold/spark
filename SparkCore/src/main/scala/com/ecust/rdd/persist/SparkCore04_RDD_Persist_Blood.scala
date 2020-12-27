package com.ecust.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 */
object SparkCore04_RDD_Persist_Blood {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    val listRDD: RDD[String] = sc.makeRDD(list, 1)
    val wordRDD: RDD[String] = listRDD.flatMap(word => word.split(" "))
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word =>
    { println("map阶段")
      (word, 1)
    })
        tupleRDD.cache()
//    tupleRDD.persist(StorageLevel.MEMORY_AND_DISK)

    sc.setCheckpointDir("./checkPoint")
    tupleRDD.checkpoint()

    val groupRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
//    val resultRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)

//    resultRDD.collect().foreach(println)
    println("------------华丽分割线----------------")
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

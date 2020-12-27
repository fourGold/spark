package com.ecust.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 */
object SparkCore03_RDD_CheckPoint {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    val listRDD: RDD[String] = sc.makeRDD(list, 1)
    val wordRDD: RDD[String] = listRDD.flatMap(word => word.split(" "))
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word => (word, 1))
//    tupleRDD.cache()
    println(tupleRDD.toDebugString)//运行之前看血缘关系

//    tupleRDD.persist(StorageLevel.MEMORY_AND_DISK)

    sc.setCheckpointDir("./checkPoint")
    tupleRDD.checkpoint()

    val groupRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    groupRDD.collect().foreach(println)
    println("----------------------------")
    println(tupleRDD.toDebugString)//运行之后看血缘
    sc.stop()
  }
}

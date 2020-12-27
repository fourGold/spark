package com.ecust.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{RDDUtilsInSpark, SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 */
object SparkCore05_RDD_CheckPointUse {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = RDDUtilsInSpark.getCheckpointRDD[RDD[(String, Int)]](sc, "./checkPoint/1186c961-ddb4-4af5-b7dc-6cc99776490b/rdd-2")

    val result: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    result.collect().foreach(println)
    sc.stop()
  }
}

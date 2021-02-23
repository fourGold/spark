package com.ecust.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{RDDUtilsInSpark, SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 * 先使用SparkCore03里面的Ck保存一个Ck,然后在这里选择路径重启吧
 */
object SparkCore05_RDD_CheckPointUse {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    //在这里使用RDDUtilsSpark进行使用,注意包
    //note:因为sc.checkpointFile(path)是private[spark]的，所以该类要写在自己工程里新建的package org.apache.spark中
    //注意路径的写入,可以使用HDFS上的路径,打包到集群运行
    val rdd: RDD[(String, Int)] = RDDUtilsInSpark.getCheckpointRDD[RDD[(String, Int)]](sc, "./checkPoint/1186c961-ddb4-4af5-b7dc-6cc99776490b/rdd-2")

    val result: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    result.collect().foreach(println)
    sc.stop()
  }
}

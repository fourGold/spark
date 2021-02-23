package com.ecust.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-26 10:52
 * 自定义分区规则
 */
object SparkCore02_RDD_How2Partition {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)

    val value: RDD[String] = sc.textFile("SparkCore/src/main/scala/com/ecust/rdd/partition/PartitionTest.txt", 3)
    value.saveAsTextFile("SparkCore/src/main/scala/com/ecust/rdd/partition/PartitionTestOut.txt")
    //todo 问题:三行数据123 456 7,却划分了4个区

    //数据本质123[4]567[8]9[10] 一共10个字节 10/3 = 3-1 划分4个区0-3,3-6,6-9,10
    //第一个文件 : 123[4] 一次读进来一行 检测offset到3时另起文件 已经检测到3 => 123
    //第二个文件 : 567[8] 已经检测到6 =>456
    //第三个文件 : 9[10] 已经检测到9 =>7
    //第四个文件 : 空
    sc.stop()
  }
}

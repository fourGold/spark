package com.ecust.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-24 14:03
 */
object SparkCore02_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountPersist").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Spark", "Hello Scala")

    //生成RDD RDD中不存储数据
    val listRDD: RDD[String] = sc.makeRDD(list, 1)
    val wordRDD: RDD[String] = listRDD.flatMap(word => word.split(" "))
    val tupleRDD: RDD[(String, Int)] = wordRDD.map(word =>
    { println("map阶段")
      (word, 1)
    })

//    tupleRDD.cache()
    tupleRDD.persist(StorageLevel.MEMORY_AND_DISK)
    //memory_only当内存不够的情况下,数据不能溢写到磁盘,会丢失数据
    //memory_and_disk当内存不够的情况下,会将数据落到磁盘
    //持久化操作,必须在行动算子执行时,完成的
    sc.setCheckpointDir("./checkPoint")
    //一般要保存到分布式存储中
    tupleRDD.checkpoint()
    //检查点路径,在作业执行完毕之后也是不会删除的

    //分组的操作
    val groupRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
    val resultRDD: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _)

    resultRDD.collect().foreach(println)
    println("------------华丽分割线----------------")
    groupRDD.collect().foreach(println)
    sc.stop()
  }
}

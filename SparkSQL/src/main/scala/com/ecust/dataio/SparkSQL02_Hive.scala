package com.ecust.dataio

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 * 存在bug
 */
object SparkSQL02_Hive {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse").getOrCreate()

    //使用SparkSQL连接外置的Hive
    //1.拷贝Hive-site.xml到classpath下
    //2.启用Hive支持
    //3.增加对应的依赖关系(包含musql驱动)
    spark.sql("show databases").show()

    //关闭环境
    spark.close()
  }
}

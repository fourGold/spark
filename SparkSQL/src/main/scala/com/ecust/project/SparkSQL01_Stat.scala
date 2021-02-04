package com.ecust.project

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 */
object SparkSQL01_Stat {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    import spark.implicits._

    val properties: Properties = PropertiesUtil.load("sql.properties")

    // 3 连接外部Hive，并进行操作
    spark.sql("show databases").show()
    spark.sql("use test")
    spark.sql("show tables").show()

    spark.sql(properties.getProperty("sql")).show()
    // 4 释放资源
    spark.stop()
  }
}

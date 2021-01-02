package com.ecust.dataio

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 * 存在bug
 */
object SparkSQL01_MySQL_BUG {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取mysql数据
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info1")
      .load()
//    df.show

    import spark.implicits._
    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info1")
      .mode(SaveMode.Overwrite)
      .save()

    //关闭环境
    spark.close()
  }
}

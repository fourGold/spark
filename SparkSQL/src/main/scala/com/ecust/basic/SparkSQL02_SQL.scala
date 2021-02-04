package com.ecust.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BASIC")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("./input/user.json")
//    dataFrame.show()

    //将数据创建临时表
    dataFrame.createOrReplaceTempView("user")
    //view只能查不能改

    sparkSession.sql(
      """
        |select * from user
        |""".stripMargin).show()

    //将年龄加1
    sparkSession.sql(
      """
        |select name,age + 1 as age from user
        |""".stripMargin).show()


    //关闭环境
    sparkSession.close()
  }
}

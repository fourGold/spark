package com.ecust.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 */
object SparkSQL01_DSL {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BASIC")
    //SparkSession底层封装了SparkContext
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("./input/user.json")
//    dataFrame.show()

    //DSL语句使用调用方法,类似于Flink总的TABLE API与SQL API
    dataFrame.select("name","age").show()

    import sparkSession.implicits._
    dataFrame.select($"age"+1).as("age").show()
    //怎么选择两列
    //可以使用单引号代表引用
    dataFrame.select('age+1).as("age").show()

    //关闭环境
    sparkSession.close()
  }
}

package com.ecust.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 * 弱类型操作,只有0,1 没有类型的概念
 * 没有类型的概念
 * 强类型通过属性操作,跟属性没关系
 * 自定属性类,定义泛型
 */
object SparkSQL03_UDAF {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BASIC")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    //聚合函数也是比较重要的,比如,平均值,最大值,最小值
    //DataFrame
    val dataFrame: DataFrame = sparkSession.read.json("./input/user.json")
//    dataFrame.show()

    //将数据创建临时表
    dataFrame.createOrReplaceTempView("user")
    //view只能查不能改

    sparkSession.udf.register("prefixName",(name:String)=>{"name+"+name})

    //将某一字段的名字加上前缀
    sparkSession.sql(
      """
        |select prefixName(name),age from user
        |""".stripMargin).show()

    //使用udaf-aggregator函数
    val myAvg = new MyAvg()
    sparkSession.udf.register("myAvg",functions.udaf(new MyAvgAgg))
    sparkSession.sql(
      """
        |select myAvg(age) as avgAge from user
        |""".stripMargin).show()

    //关闭环境
    sparkSession.close()
  }
}

package com.ecust.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 */
object SparkSQL01_UDF {
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

    sparkSession.udf.register("prefixName",(name:String)=>{"name+"+name})

    sparkSession.sql(
      """
        |select prefixName(name),age from user
        |""".stripMargin).show()

    //关闭环境
    sparkSession.close()
  }
}

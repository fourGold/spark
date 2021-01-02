package com.ecust.basic

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author Jinxin Li
 * @create 2021-01-01 13:29
 */
object SparkSQL04_RDD2DF {
  def main(args: Array[String]): Unit = {
    //创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BASIC")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val list = List((1, "张三", 30), (2, "李四", 40))

    //sparkSession内部封装了sparkContext,所以能够直接点出来
    val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(list)

    //这里将RDD转换为DF,需要添加结构
    println("rdd-->df")
    val df: DataFrame = rdd.toDF("id", "name", "age")
    df.show(2)

    //将df转换为rdd,会将格式改变,可以发现,把表结构进行了封装
    val rdd2: RDD[Row] = df.rdd

    //将DF->DS 缺少业务类型
    println("df-->ds")
    val ds: Dataset[User] = df.as[User]
    ds.show()

    //DS->DF 直接转换即可
    println("ds-->df")
    val df1: DataFrame = ds.toDF()
    df1.show()

    //RDD-->DS
    println("rdd-->ds")
    //rdd想直接转ds
    val ds1: Dataset[(Int, String, Int)] = rdd.toDS()
    ds1.show()

    //可以把rdd转换为特定类型
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    ds2.show()

    //DS-->RDD
    val rdd1: RDD[User] = ds2.rdd

    //关闭环境
    sparkSession.close()
  }
}

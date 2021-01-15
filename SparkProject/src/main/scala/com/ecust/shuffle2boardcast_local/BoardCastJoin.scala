package com.ecust.shuffle2boardcast_local

import com.ecust.shuffle2narrow_local.{CategoryCountAccumulator, UserVisitAction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author JueQian
 * @create 01-15 19:58
 * 需求,将pd中的品牌名放到订单表中
 */
object BoardCastJoin {
  def main(args: Array[String]): Unit = {

    //0x0 创建配置获得数据
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val order: RDD[String] = sc.textFile("./input/order.txt")
    val pd: RDD[String] = sc.textFile("./input/pd.txt")

    //0x1 转换样例类
    val orderRDD: RDD[Order] = order.map(info => {
      val fields: Array[String] = info.split("\t")
      Order(fields(0), fields(1), fields(2),"-")
    })

    //0x2 小表数据放入map中
    val map: mutable.Map[String, String] = mutable.Map(("01","小米"), ("02","华为"),("03","格力"))

    // todo 如何通过读文件的方式把数据放入map?????
//    pd.foreach(info=>{
//      val fields: Array[String] = info.split(" ")
//      map.put(fields(0),fields(1))
//    })

    //0x3 定义,注册,使用广播变量
    val boardCast= sc.broadcast(map)
    //定义广播变量
    val result: RDD[Order] = orderRDD.mapPartitions(iter => {
      val iterator = iter.map(order => {
          val map: mutable.Map[String, String] = boardCast.value
          Order(order.orderId, order.pid, order.count, map.getOrElse(order.pid, "-"))
      })
      iterator
    })

    //0x4 打印
    result.collect().foreach(println)


    //0x5 关闭连接
    sc.stop()
  }
}

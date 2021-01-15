package com.ecust.shuffle2narrow_yarn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.parallel.{immutable, thresholdFromSize}

/**
 * @author JueQian
 * @create 01-15 19:11
 * 求热门品类前10 将全部动作都淹没掉
 */
object HotTop10 {
    def main(args: Array[String]): Unit = {

      //0x0 创建配置获得数据
      val conf: SparkConf = new SparkConf().setAppName("SparkYarnTest")
      val sc: SparkContext = new SparkContext(conf)
      val lineRDD: RDD[String] = sc.textFile("hdfs://hadoop102/input/user_visit_action.txt")

      //0x1 转换样例类
      val actionRDD: RDD[UserVisitAction] = lineRDD.map {
        line => {
          val datas: Array[String] = line.split("_")

          UserVisitAction(
            datas(0),
            datas(1).toLong,
            datas(2),
            datas(3).toLong,
            datas(4),
            datas(5),
            datas(6).toLong,
            datas(7).toLong,
            datas(8),
            datas(9),
            datas(10),
            datas(11),
            datas(12).toLong
          )
        }
      }

      //0x3 创建,注册,使用,获取累加器结果 结果是一个Map
      val acc: CategoryCountAccumulator = new CategoryCountAccumulator()
      sc.register(acc, "CategoryCountAccumulator")
      actionRDD.foreach(action => acc.add(action))
      val accMap: mutable.Map[(String, String), Long] = acc.value

      //0x4 获取值后把动作淹没掉,将动作+品类结合视为一个热度,直接排序求前10的品类+动作 打印
      val stringToLong = accMap.map(info => {
        (info._1._1 + "-" + info._1._2, info._2)
      })

        //todo 注意倒数的函数科里化
      val tuples= stringToLong.toList.sortBy(info => info._2)(Ordering[Long].reverse).take(10)
      tuples.foreach(println)

      //4.关闭连接
      sc.stop()
    }
}

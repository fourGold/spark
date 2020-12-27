package com.ecust.rdd.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Jinxin Li
 * @create 2020-12-26 11:42
 * 少加:没有行动算子
 * 多加:两个行动算子
 */
object SparkCore05_RDD_MyAccumulator {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello rdd"),1)

//    rdd.map(n=>(n,1)).reduceByKey(_+_).collect().foreach(println)


    //自定义数据累加器:wordCount
    /*
    继承AccumulatorV2,定义泛型
    in:累加器输入的数据类型 String
    out:累加器返回的数据类型 mutable.Map[String,Long]
     */
    val wcAcc = new MyAccumulatorWc()

    sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(
      word=>{
        //数据的累加(使用累加器)
        wcAcc.add(word)
      }
    )
    println(wcAcc.value)

    sc.stop()
  }

  class MyAccumulatorWc extends AccumulatorV2[String,mutable.Map[String,Long]]{
    private var wcMap = mutable.Map[String,Long]()

    override def isZero: Boolean = wcMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulatorWc()
    }

    override def reset(): Unit = wcMap.clear()

    //获取累加器需要计算的值
    override def add(v: String): Unit = {
      val newCnt = wcMap.getOrElse(v,0L)+1
      wcMap.update(v,newCnt)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.wcMap
      val map2=other.value

      map2.foreach{
        case (word,count)=>{
          val newCount = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = wcMap
  }
}

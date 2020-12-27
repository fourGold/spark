package com.ecust.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author Jinxin Li
 * @create 2020-12-26 10:52
 * 自定义分区规则
 */
object SparkCore01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxxxxxxxxxxx"),
      ("wba", "aaaaaaaaaaaaaa"),
      ("cba", "dddddddddddd"),
      ("wcba", "ppppppppppppppppppppppp")
    ), 3)

    /*
    自动义分区器,决定数据去哪个分区
     */
    val rddPar: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner())

    rddPar.saveAsTextFile("./par")

    sc.stop()
  }

  class MyPartitioner extends Partitioner{
    //分区数量
    override def numPartitions: Int = 3

    //返回Int类型,返回数据的分区索引 从零开始
    //Key表示数据的KV到底是什么
    //根据数据的key值返回数据所在分区索引
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }

      /*if (key == "nba"){
        0
      } else if(key == "cba"){
        1
      }else{
        2
      }*/
    }
  }

}

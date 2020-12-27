package org.apache.spark

import org.apache.spark.rdd.RDD

/**
 * @author Jinxin Li
 * @create 2020-12-26 11:15
 */

object RDDUtilsInSpark {
  def getCheckpointRDD[T](sc: SparkContext, path: String) = {
    //path要到part-000000的父目录
    val result: RDD[Any] = sc.checkpointFile(path)
    result.asInstanceOf[T]
  }
}

package com.ecust.shuffle2narrow_local

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author JueQian
 * @create 01-15 19:13
 */
//创建map 存储值为 k-v : (品类名,品类动作),动作发生次数
//e: (鞋子,click),100
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {

  var map = mutable.Map[(String, String), Long]()

  //传入为空的方法
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new CategoryCountAccumulator()
  }

  override def reset(): Unit = map.clear()

  //获取累加器需要计算的值
  override def add(action: UserVisitAction): Unit = {

    if (action.click_category_id != -1) {
      val key = (action.click_category_id.toString, "click")

      map(key) = map.getOrElse(key, 0L) + 1L
    } else if (action.order_category_ids != "null") {
      val ids: Array[String] = action.order_category_ids.split(",")

      for (id <- ids) {
        val key = (id, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    } else if (action.pay_category_ids != "null") {
      val ids: Array[String] = action.pay_category_ids.split(",")

      for (id <- ids) {
        val key = (id, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
  }

  //Driver合并多个累加器
  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {

    other.value.foreach {
      case (category, count) => {
        map(category) = map.getOrElse(category, 0L) + count
      }
    }
  }

  //累加器结果
  override def value: mutable.Map[(String, String), Long] = map
}

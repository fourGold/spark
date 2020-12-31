package com.ecust.saprkcore

/**
 * @author Jinxin Li
 * @create 2020-12-31 13:55
 */
class Task extends Serializable {//实现序列化 特质
  //包含原数据的数据结构
  val data = List(1, 2, 3, 4)

  val function: Int => Int = (num: Int) => {
    num * 2
  }

  //注意函数的类型是Int=>Int
  val logic:Int=>Int = _*2

  //计算任务
  def  compute() ={
    data.map(logic)
  }
}

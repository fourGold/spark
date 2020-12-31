package com.ecust.saprkcore

/**
 * @author Jinxin Li
 * @create 2020-12-31 14:12
 */
class SubTask extends Serializable {
  //这是一种特殊的数据结构,其中包含了数据的格式,数据的计算逻辑与算子转换
  //接收到数据之后,可以进行计算
  //RDD 广播变量 累加器 就是类似的数据结构
  var data :List[Int] = _
  var logic:Int=>Int = _

  //计算任务
  def  compute() ={
    data.map(logic)
  }
}

package com.ecust.shuffle2narrow_local

/**
 * @author JueQian
 * @create 01-15 19:22
 */
//创建结果表
case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数


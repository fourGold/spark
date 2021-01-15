package com.ecust.shuffle2boardcast_local

/**
 * @author JueQian
 * @create 01-15 19:12
 */
//用户动作样例类
case class Order(
                  orderId: String,
                  pid: String,
                  count: String, //数目
                  name: String
                )


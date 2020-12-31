package com.ecust.saprkcore

import java.io.{ObjectOutputStream, OutputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author Jinxin Li
 * @create 2020-12-31 13:43
 */
object Driver {
  def main(args: Array[String]): Unit = {

    //进行逻辑的封装,计算的准备,数据的提交

    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val out1: OutputStream = client1.getOutputStream
    val out2: OutputStream = client2.getOutputStream

    val objOut1 = new ObjectOutputStream(out1)
    val objOut2 = new ObjectOutputStream(out2)

    val  task = new Task()

    val subTask1 = new SubTask()
    subTask1.logic=task.logic
    subTask1.data=task.data.take(2)

    val subTask2 = new SubTask()
    subTask2.logic=task.logic
    subTask2.data=task.data.takeRight(2)

    objOut1.writeObject(subTask1)
    objOut1.flush()
    objOut1.close()

    objOut2.writeObject(subTask2)
    objOut2.flush()
    objOut2.close()

    //发送,注意在网络中传递的数据要进行序列化,不可能传递对象,必须序列化
    println("任务发送完毕")

    //关闭客户端
    client1.close()
    client2.close()
  }
}

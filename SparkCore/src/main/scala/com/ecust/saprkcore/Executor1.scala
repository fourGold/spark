package com.ecust.saprkcore

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

import jline.internal.ShutdownHooks.Task

/**
 * @author Jinxin Li
 * @create 2020-12-31 13:44
 */
object Executor1 {
  def main(args: Array[String]): Unit = {

    //启动服务器,接受数据
    val server = new ServerSocket(9999)

    println("服务器9999启动,等待接受数据...")

    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream

    val objIn = new ObjectInputStream(in)

    val task = objIn.readObject().asInstanceOf[SubTask]

    val ints: List[Int] = task.compute()

    println("接收到客户端9999接受的数据:"+ints)

    objIn.close()
    client.close()
    server.close()
  }
}

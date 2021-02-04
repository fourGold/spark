package com.ecust.project

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Jinxin Li
 * @create 2020-11-25 19:15
 */
object PropertiesUtil {
  def main(args: Array[String]): Unit = {

  }
  def load(name:String)={
    val properties = new Properties()
    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(name)))
    properties
  }
}

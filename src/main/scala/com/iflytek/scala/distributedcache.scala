package com.iflytek.scala

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object distributedcache {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    //1:注册文件
    env.registerCachedFile("data.txt","bieming.txt")

    val data = env.fromElements("a","b","c","d")

//    所有Flink函数类都有其Rich版本。它与常规函数的不同在于，
//    可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
//    Rich Function有一个生命周期的概念。典型的生命周期方法有：
// open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
// close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
// getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
    val result = data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext
          .getDistributedCache
          .getFile("bieming.txt")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext){
          val line = it.next()
          println("line:"+line)
        }
      }
      override def map(value: String) = {
        value
      }
    })

    result.print()
  }

}

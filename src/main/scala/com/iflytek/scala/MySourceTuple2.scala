package com.iflytek.scala

import java.util.Random

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * 每隔1秒发送一个tuple2类型的数据，第一个字段值为随机的一个姓氏，第二个字段为自增的数字
  **/
class MySourceTuple2 extends SourceFunction[(String, Long)] {

  var isRunning: Boolean = true
  val names: List[String] = List("张", "王", "李", "赵")
  private val random = new Random()
  var number: Long = 1

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index: Int = random.nextInt(4)
      ctx.collect((names(index), number))
      number += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

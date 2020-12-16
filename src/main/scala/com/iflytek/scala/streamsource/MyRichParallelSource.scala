package com.iflytek.scala.streamsource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class MyRichParallelSource extends RichParallelSourceFunction[Long]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[Long]) = {
    while(isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  override def cancel()= {
    isRunning = false
  }

  /**
    * 这个方法只会在最开始的时候被调用一次
    * 实现获取链接的代码
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  /**
    * 实现关闭链接的代码
    */
  override def close(): Unit = {
    super.close()
  }
}

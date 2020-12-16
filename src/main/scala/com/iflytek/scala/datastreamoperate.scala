package com.iflytek.scala

import com.iflytek.scala.streamsource.MyNoParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object datastreamoperate {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

//    val text = env.addSource(new MyNoParallelSource)
//    val mapData = text.map(line=>{
//      println("原始接收到的数据："+line)
//      line
//    }).filter(_ % 2 == 0)
//    val sum = mapData.map(line=>{
//      println("过滤之后的数据："+line)
//      line
//    }).timeWindowAll(Time.seconds(2)).sum(0)

    val text1 = env.addSource(new MyNoParallelSource)
    val text2 = env.addSource(new MyNoParallelSource)

//    val unionall = text1.union(text2)
//    val sum = unionall.map(line=>{
//      println("接收到的数据："+line)
//      line
//    }).timeWindowAll(Time.seconds(2)).sum(0)
//    sum.print().setParallelism(1)

    val text2_str = text2.map("str" + _)
    val connectedStreams = text1.connect(text2_str)
    val result = connectedStreams.map(
      line1=>{line1},
      line2=>{line2} )
    result.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")
  }

}

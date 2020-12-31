package com.iflytek.scala.datastream

import java.util.Random

import com.iflytek.scala.MySourceTuple2
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object reduceFunctiondemo {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .addSource(new MySourceTuple2)
      .keyBy(_._1)
      .reduce((a,b)=>{(a._1,a._2+b._2)})
        .print("处理结果：")

    env.execute()
  }

}
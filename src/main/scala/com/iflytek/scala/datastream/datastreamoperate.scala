package com.iflytek.scala.datastream

import java.util.Random

import com.iflytek.scala.MySourceTuple2
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object datastreamoperate {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .addSource(new MySourceTuple2)
      .map(x=>{(x._1+"_test",x._2)})
      .print()

    env.execute()
  }

}
package com.iflytek.scala

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

object accumlator {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = env.fromElements("a","b","c","d")

    val res = data.map(new RichMapFunction[String,String] {
      //1：定义累加器
      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2:注册累加器
        getRuntimeContext.addAccumulator("num-lines",this.numLines)
      }

      override def map(value: String) = {
        //3：使用累加器
        this.numLines.add(1)
        value
      }
    }).setParallelism(4)
    res.writeAsText("count20",FileSystem.WriteMode.OVERWRITE)
    val jobResult = env.execute("BatchDemoCounterScala")
    //4：获取累加器
    val num = jobResult.getAccumulatorResult[Int]("num-lines")
    println("num:"+num)

  }

}

package com.iflytek.scala

import com.iflytek.scala.streamsource.MyNoParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingDemoMyPartitioner {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    //隐式转换
    import org.apache.flink.api.scala._
    val text = env.addSource(new MyNoParallelSource)

    //把long类型的数据转成tuple类型
    val tupleData = text.map(line=>{
      Tuple1(line)// 注意tuple1的实现方式
      // Tuple2 可以直接写成如 (line,1)
      // 但是 Tuple1 必须加上关键词 Tuple1
    })

    // 上面将 Long 转换为 Tuple1[Long] 的原因是由于
    // partitionCustom 的 field 参数类型: Tuple1[K]
    val partitionData = tupleData.partitionCustom(new MyPartitioner, 0)

    val result = partitionData.map(line=>{
      println("当前线程id："+
        Thread.currentThread().getId+",value: "+line)
      line._1
    })
    result.print().setParallelism(1)
    env.execute("StreamingDemoWithMyNoParallelSourceScala")
  }
}

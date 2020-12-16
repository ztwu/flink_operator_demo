package com.iflytek.scala

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object datastreamnsinksource {

//  Flink内置Connctors
//  ApacheKafka(source/sink)
  //Apache Cassandra (sink)
  //Elasticsearch (sink)
  //Hadoop FileSystem (sink)
  //RabbitMQ (source/sink)
  //Apache ActiveMQ (source/sink)
  //Redis (sink)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //隐式转换
    import org.apache.flink.api.scala._

    val data = List(10, 15 , 20)
    val text = env.fromCollection(data)

//    val text = env.readTextFile("test.txt")
//    env.socketTextStream("localhost", 9999)

    //针对map接收到的数据执行加1的操作
    val num = text.map(_.toInt + 1)

    num.print().setParallelism(2)

    num.writeAsText("datastream.txt", FileSystem.WriteMode.OVERWRITE)

    num.map(x=>(x,1)).writeAsCsv("datastream.csv", FileSystem.WriteMode.OVERWRITE, "\n",",")

    env.execute("StreamingFromCollectionScala")
  }

}

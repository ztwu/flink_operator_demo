package com.iflytek.scala.TableandSqlapi

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}

import scala.collection.mutable

/**
  * @Description Table与DataStream的相互转化
  *
  **/
object TableToDataStream {

  def main(args: Array[String]): Unit = {
    //模拟输入数据
    val data = Seq("hello", "hadoop", "flink", "hello", "spark", "hello", "storm", "hello", "flink", "flink")
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(streamEnv)
    //将DataStream转换为table
    val source: Table = streamEnv.fromCollection(data).toTable(tableEnv, 'word)

//    //业务逻辑
    val tableResult: Table = source.groupBy('word) //分组
      .select('word, 'word.count) //统计
    //使用缩进模式将table转换为DataStream，它用true或false来标记数据的插入和撤回，true（数据不存在）代表插入，false（数据已存在）代表撤回
    val result: DataStream[(Boolean, (String, Long))] = tableEnv.toRetractStream[(String, Long)](tableResult)

    //自定义Sink进行输出
//    result.addSink(new MySink())
    result.print()
    streamEnv.execute()

  }
}

//自定义Sink，输出插入进来的数据
class MySink extends RichSinkFunction[(Boolean, (String, Long))] {
  private var resultSet: mutable.Set[(String, Long)] = _

  //初始执行一次
  override def open(parameters: Configuration): Unit = {
    //初始化内存存储结构
    resultSet = new mutable.HashSet[(String, Long)]
  }

  //每个元素执行一次
  override def invoke(v: (Boolean, (String, Long)), context: SinkFunction.Context[_]): Unit = {
    //主要逻辑
    if (v._1) {
      resultSet.add(v._2)
    }
  }

  //最后执行一次
  override def close(): Unit = {
    //打印
    resultSet.foreach(println)
  }
}

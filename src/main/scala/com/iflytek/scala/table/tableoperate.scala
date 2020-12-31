package com.iflytek.scala.table

import java.util.Random

import com.iflytek.scala.MySourceTuple2
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala._

case class user(name:String, num:Long)

object tableoperate {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(streamEnv)
    val datas = streamEnv
      .addSource(new MySourceTuple2)
      .map(x=>{(x._1+"_test",x._2)})

//    获取table
//    val source = tableEnv.fromDataStream(datas,'name, 'num)
    tableEnv.registerDataStream("test", datas,'name, 'num)
    val source = tableEnv.scan("test")

    //    //业务逻辑
    val tableResult: Table = source.groupBy('name) //分组
      .select('name, 'name.count) //统计
    val result= tableEnv.toRetractStream[user](tableResult)

    result.print()
    streamEnv.execute()
  }

}
package com.iflytek.scala.dataset

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ListBuffer

object Aggdemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()  // 相当于 Java 中的 Array

    data.append("hello you")
    data.append("hello me")
    val data5 = ListBuffer[Tuple2[Int,String]]()
    data5.append((2,"zs"))
    data5.append((4,"xw"))
    data5.append((3,"ww"))
    data5.append((1,"xw"))
    data5.append((1,"ww"))
    data5.append((1,"xw"))
    val text5 = env.fromCollection(data5)
    text5.groupBy(1).aggregate(Aggregations.SUM, 0).print()

  }

}

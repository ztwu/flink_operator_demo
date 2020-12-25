package com.iflytek.scala.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.collection.mutable.ListBuffer

object reducedemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()  // 相当于 Java 中的 Array

    data.append("hello you")
    data.append("hello me")
    val data5 = ListBuffer[Tuple2[Int,String]]()
    data5.append((2,"zs"))
    data5.append((4,"ls"))
    data5.append((3,"ww"))
    data5.append((1,"xw"))
    data5.append((1,"aw"))
    data5.append((1,"mw"))
    val text5 = env.fromCollection(data5)
    text5.groupBy(0).reduce((a,b)=>{(a._1,a._2+b._2)}).print()

  }

}

package com.iflytek.scala

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object datasetoperate {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = ListBuffer[String]()  // 相当于 Java 中的 Array

    data.append("hello you")
    data.append("hello me")

    val text = env.fromCollection(data.toList)

    text.mapPartition(it=>{
      //创建数据库连接，建议吧这块代码放到try-catch代码块中
      val res = ListBuffer[String]()
      while(it.hasNext){
        val line = it.next()
        val words = line.split("\\W+")
        for(word <- words){
          res.append(word)
        }
      }
      res
      //关闭连接
    }).print()

    val flatMapData = text.flatMap(line=>{
      val words = line.split("\\W+")
      for(word <- words){
        println("单词："+word)
      }
      words
    })

    // 如果数据类型是Tuple,可以指定按Tuple某个字段去重
    flatMapData.distinct()
      .print()


    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"zs"))
    data1.append((2,"ls"))
    data1.append((3,"ww"))


    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"beijing"))
    data2.append((2,"shanghai"))
    data2.append((3,"guangzhou"))

    val text1 = env.fromCollection(data1)
    val text2 = env.fromCollection(data2)

    text1.join(text2)
      .where(0)
      .equalTo(0)
      .apply((first,second)=>{
        (first._1,first._2,second._2)
      })
      .print()

    text1.leftOuterJoin(text2)
      .where(0)
      .equalTo(0)
      .apply((first,second)=>{
        if(second==null){
          (first._1,first._2,"null")
        }else{
          (first._1,first._2,second._2)
        }
    }).print()

    text1.leftOuterJoin(text2)
      .where(0)
      .equalTo(0)
      .apply((first,second)=>{
        if(first==null){
          (second._1,second._2,"null")
        }else{
          (second._1,second._2,second._2)
        }
    }).print()

    text1.fullOuterJoin(text2)
      .where(0)
      .equalTo(0)
      .apply((first,second)=>{
        if(first==null){
          (second._1,"null",second._2)
        }else if(second==null){
          (first._1,first._2,"null")
        }else{
          (first._1,first._2,second._2)
        }
    }).print()

    val data11 = List("zs","ww")
    val data21 = List(1,2)

    val text11 = env.fromCollection(data11)
    val text21 = env.fromCollection(data21)

    text11.cross(text21).print()

    text1.union(text2).print()

    val data5 = ListBuffer[Tuple2[Int,String]]()
    data5.append((2,"zs"))
    data5.append((4,"ls"))
    data5.append((3,"ww"))
    data5.append((1,"xw"))
    data5.append((1,"aw"))
    data5.append((1,"mw"))
    val text5 = env.fromCollection(data5)

    //获取前3条数据，按照数据插入的顺序
    text5
      .first(3)
      .print()
    println("==============================")

    //根据数据中的第一列进行分组，获取每组的前2个元素
    text5
      .groupBy(0)
      .first(2)
      .print()
    println("==============================")

    //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
    text5
      .groupBy(0)
      .sortGroup(1,Order.ASCENDING)
      .first(2)
      .print()
    println("==============================")

    //不分组，全局排序获取集合中的前3个元素，
    text5
      .sortPartition(0,Order.ASCENDING)
      .sortPartition(1,Order.DESCENDING)
      .first(3)
      .print()

  }

}

package com.iflytek.scala

object scalatest {
  def main(args: Array[String]): Unit = {

    val data = List(1,2,3)
    for(item <- data){
      println(item)
    }
    for(i <- 0 to (data.length-1)){
      println(i,data(i))
    }


    val it2 = data.iterator
    while (it2.hasNext){
      println(it2.next())
    }

    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")
    while (it.hasNext){
      println(it.next())
    }

    val data2 = (1,2,3,4)
    data2.productIterator.foreach{x=>println(x)}

    val it3 = data2.productIterator
    while (it3.hasNext){
      println(it3.next())
    }

  }
}

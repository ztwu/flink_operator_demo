package com.iflytek.scala

import com.iflytek.scala.outputformat.{HBaseOutputFormat, KafkaOutputFormat}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object datasetsinksource {
  def main(args: Array[String]): Unit = {

    val inputPath = "data.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment

//    val text = env.readTextFile(inputPath)
//    val counts = text
//      .flatMap(x=>{x.toLowerCase.split(",")})
//      .filter(_.nonEmpty)
//      .map(x=>(x,1))
//      .groupBy(0)
//      .sum(1)

    val text = env.createInput(
      JDBCInputFormat.buildJDBCInputFormat()
      // 设置驱动、url、用户名、密码以及查询语句
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://localhost:3306/flink")
      .setUsername("root")
      .setPassword("root")
      .setQuery("select word from word")
      // 说明每条记录每个字段的数据类型
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO))
      .finish())

    val counts = text
      .flatMap(x=>{
        x.getField(0).toString.toLowerCase.split(",")
      })
      .filter(_.nonEmpty)
      .map(x=>(x,1))
      .groupBy(0)
      .sum(1)

//    counts
//      .writeAsCsv("output6","\n",
//        "#", FileSystem.WriteMode.OVERWRITE)
//      .setParallelism(2)

//    counts
//      .writeAsText("output7", FileSystem.WriteMode.OVERWRITE)
//      .setParallelism(2)

    //2.定义数据
//    val dataSet: DataSet[String] = env.fromElements("103,zhangsan,20", "104,lisi,21", "105,wangwu,22", "106,zhaolilu,23")
//    dataSet
//      .output(new HBaseOutputFormat)
//      .setParallelism(2)

    val dataSet: DataSet[String] = env.fromElements("103,zhangsan,20", "104,lisi,21", "105,wangwu,22", "106,zhaolilu,23")
      dataSet
        .output(new KafkaOutputFormat)

    env.execute("batch wordCount")
  }

  case class WordWithCount(word: String, count: Long)

}

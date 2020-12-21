package com.iflytek.scala.TableandSqlapi

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sources.CsvTableSource

/**
  * @Description Table与DataSet的相互转化
  *
  **/
object TableDataSet {

  def main(args: Array[String]): Unit = {

    // 获取编程入口
    val batchEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(batchEnvironment)
    val csvTableSourcePath: String = "student.txt"
    val source: CsvTableSource = new CsvTableSource(
      csvTableSourcePath,
      Array[String]("id", "name", "sex", "age", "department"),
      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING)
      , fieldDelim = ","
      , ignoreFirstLine = false
    )
    //将数据源注册为table
    tableEnv.registerTableSource("student", source)
    val resultTable: Table = tableEnv.sqlQuery("select id,name,sex,age,department from student")
    //将table转为DataSet
    val studentDataSet: DataSet[(Int, String, String, Int, String)] = tableEnv.toDataSet[(Int, String, String, Int, String)](resultTable)
    //将DataSet转为table
    println("-------------按默认名称将DataSet转换为Table-------------")
    val studentTable1: Table = tableEnv.fromDataSet(studentDataSet)
    studentTable1.printSchema()

    println("------------将DataSet中的id与name转换为Table--------------")
    val studentTable2: Table = tableEnv.fromDataSet(studentDataSet, 'id, 'name)
    studentTable2.printSchema()

    println("---------------按指定顺序将DataSet转换为Table--------------")
    val studentTable3: Table = tableEnv.fromDataSet(studentDataSet, '_4, '_3, '_5, '_2, '_1)
    studentTable3.printSchema()

  }
}

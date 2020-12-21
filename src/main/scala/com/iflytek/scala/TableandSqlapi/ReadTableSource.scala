package com.iflytek.scala.TableandSqlapi

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

/**
  * @Description Flink Table API——使用CSVTableSource方式读取数据
  *
  **/
object ReadTableSource {

  def main(args: Array[String]): Unit = {
    //创建table编程入口
    val datasetEnv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(datasetEnv)
    val csvTableSourcePath: String = "student.txt"

    val source1: CsvTableSource = new CsvTableSource(
      csvTableSourcePath,
      Array[String]("id", "name", "sex", "age", "department"),
      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING)
      , fieldDelim = ","
      , ignoreFirstLine = false
    )

    val source2: CsvTableSource = CsvTableSource
      .builder()
      .path(csvTableSourcePath)
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("sex", Types.STRING)
      .field("age", Types.INT)
      .field("department", Types.STRING)
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .ignoreFirstLine
      .ignoreParseErrors
      .build()

    //第一种方式
    val resultTable: Table = selectAll(tableEnv, source1, "student1")
    //第二种方式
    selectAll(tableEnv, source2, "student2")

    //将数据从一个数据源导出到另一个数据源
    val fieldNames: Array[String] = Array("id", "name", "sex", "age", "department")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING)
    //创建一个TableSink
    val csvSink: CsvTableSink = new CsvTableSink("flinkout/tablesource/student.txt", ",", 1, WriteMode.OVERWRITE)
    tableEnv.registerTableSink("student3", fieldNames, fieldTypes, csvSink)
    //插入数据
    resultTable.insertInto("student3")
    datasetEnv.execute()
  }

  private def selectAll(tableEnv: BatchTableEnvironment, studentCsvSource: CsvTableSource, tableName: String) = {
    //注册数据源
    tableEnv.registerTableSource(tableName, studentCsvSource)
    //DSL风格查询数据
    val resultTable1: Table = tableEnv.scan(tableName)
    //SQL风格查询数据
    val resultTable2: Table = tableEnv.sqlQuery("select id,name,sex,age,department from " + tableName)
    println("--------------打印表的结构----------------------")
    resultTable1.printSchema()
    // 打印输出表中的数据
    val dataset1: DataSet[Student] = tableEnv.toDataSet[Student](resultTable1)
    println("--------------DSL输出----------------------")
    dataset1.print()
    val dataset2: DataSet[Student] = tableEnv.toDataSet[Student](resultTable2)
    println("--------------SQL输出----------------------")
    dataset2.print()
    resultTable1
  }
}

case class Student(id: Int, name: String, sex: String, age: Int, department: String)

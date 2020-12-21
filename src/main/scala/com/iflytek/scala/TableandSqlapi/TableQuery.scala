package com.iflytek.scala.TableandSqlapi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types, _}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
* @Description Flink查询操作
*
**/
object TableQuery {

  def main(args: Array[String]): Unit = {

    //创建table编程入口
    val datasetEnv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(datasetEnv)
    val csvTableSourcePath: String = "student.txt"

    val source: CsvTableSource = new CsvTableSource(
      csvTableSourcePath,
      Array[String]("id", "name", "sex", "age", "department"),
      Array[TypeInformation[_]](Types.INT, Types.STRING, Types.STRING, Types.INT, Types.STRING)
      , fieldDelim = ","
      , ignoreFirstLine = false
    )

    tableEnv.registerTableSource("student", source)


    println("-----------------DSL：统计每个部门大于18岁的人数--------------------------")
    val result1: Table = tableEnv.scan("student")
      .as("id, name, sex, age, department")
      .filter("age>18")
      .groupBy("department")
      .select("department, id.count as total")
    tableEnv.toDataSet[Row](result1).print()


    println("-----------------DSL：统计每个部门大于18岁的人数(tick(')写法)--------------------------")
    val result2: Table = tableEnv.scan("student")
      .as("id, name, sex, age, department")
      .filter('age > 18)
      .groupBy('department)
      .select('department, 'id.count as 'total)
    tableEnv.toDataSet[Row](result2).print()


    println("-----------------DSL：找出人数大于6的部门--------------------------")
    val result3: Table = tableEnv.scan("student")
      .as("id, name, sex, age, department")
      .groupBy("department")
      .select("department, id.count as total")
      .where("total > 6")
    tableEnv.toDataSet[Row](result3).print()

    println("-----------------SQL：统计男性与女性中大于18岁的人数-------------------------")
    val result4: Table=tableEnv.sqlQuery(
      """
        select sex, count(*) as total
        from student
        where age > 18
        group by sex
      """.stripMargin)
    tableEnv.toDataSet[SexCount](result4).print()
  }
}

case class SexCount(age:String, count:Long)
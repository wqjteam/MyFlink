package com.wqj.flink1.base

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}
import org.apache.flink.table.sources.{CsvTableSource, TableSource}


case class person(id:Int,name:String,age:Int)
object TableSql {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val persondata = env.readTextFile("d://person")
  val maps = persondata.map(str=>{
    val strs = str.split(" ")
    person(strs(0).toInt,strs(1),strs(2).toInt)
  }
  )


  val tableEnv = BatchTableEnvironment.create(env)
  //'id, 'name,'age为映射的字段，因该用到了隐式转换什么的 ，第三行import 会影响到这里
  tableEnv.createTemporaryView("person", maps,'id','name','age')
  val result = tableEnv.sqlQuery("select *  from person where id = 1")
  val rows = result.collect()
  print(rows)
  env.execute("table_task")
}

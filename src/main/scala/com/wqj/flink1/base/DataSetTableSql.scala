package com.wqj.flink1.base

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment


case class person(id: Int, name: String, age: Int)

object DataSetTableSql {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    val persondata: DataSet[String] = env.readTextFile("workspace/input/person")
    import org.apache.flink.api.scala._
    val person_dset = persondata.map(str => {
      val strs: Array[String] = str.split(",")
      person(strs(0).toInt, strs(1), strs(2).toInt)
    })

    val table: Table = tableEnv.fromDataSet(person_dset)
    tableEnv.createTemporaryView("person", table)
    val result = tableEnv.sqlQuery("select *  from person where id=1")
    val rows = result.collect()
    print(rows)
    env.execute("dataset_table_task")
  }
}

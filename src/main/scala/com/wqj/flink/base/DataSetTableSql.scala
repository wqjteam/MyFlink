package com.wqj.flink.base

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.BatchTableEnvironment

import scala.beans.BeanProperty


case class Person(@BeanProperty id: Int, @BeanProperty name: String, @BeanProperty age: Int)

object DataSetTableSql {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    //    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inBatchMode().build()
    //    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val persondata: DataSet[String] = env.readTextFile("workspace/input/person")
    import org.apache.flink.api.scala._
    val person_dset = persondata.map(str => {
      val strs: Array[String] = str.split(",")
      Person(strs(0).toInt, strs(1), strs(2).toInt)
    })

    val table: Table = tableEnv.fromDataSet(person_dset)
    tableEnv.createTemporaryView("person", table)
    val result = tableEnv.sqlQuery("select *  from person where id=1")
    val rows = result.collect()
    print(rows)
    env.execute("dataset_table_task")
  }
}

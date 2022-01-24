package com.wqj.flink.base

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

object OpeaterStreamHive {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val hive = new HiveCatalog("myhive", "study", "src\\main\\resource", "2.3.4")
    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")
    val result = tableEnv.sqlQuery("select * from study.person")
    tableEnv.toRetractStream[Person](result)
    tableEnv.execute("OpeaterHive")
  }
}

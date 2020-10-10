package com.wqj.flink1.base

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object OpeaterHive {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)
    val hive = new HiveCatalog("myhive", "ods", "hive-site.xml", "2.3.7")
    tableEnv.registerCatalog("myhive",hive)
    tableEnv.useCatalog("myhive")
    val result=tableEnv.sqlQuery("select * from ods.student")
    tableEnv.execute("OpeaterHive")
  }
}

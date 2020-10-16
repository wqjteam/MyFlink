package com.wqj.flink1.base

import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.api.scala._


object OpeaterBatchHive {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tableEnv = TableEnvironment.create(settings)
    val hive = new HiveCatalog("myhive", "study", "D:\\develop_disk\\java\\MyFlink\\src\\main\\resource", "2.3.4")
    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")
    val result = tableEnv.sqlQuery("select * from study.person")
    tableEnv.sqlUpdate("insert into study.person_bak select * from study.person")
    tableEnv.execute("OpeaterHive")



  }
}

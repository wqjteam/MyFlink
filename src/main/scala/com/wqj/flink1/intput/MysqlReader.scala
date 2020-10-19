package com.wqj.flink1.intput

import java.sql
import java.sql.{DriverManager, ResultSet}

import com.google.gson.JsonObject
import org.apache.flink.configuration.{ConfigOption, Configuration}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.collection.mutable

class MysqlReader extends RichSourceFunction[String] {

  var conn: sql.Connection = _
  var selectStnt: sql.PreparedStatement = _
  var isRunning = true

  import org.apache.flink.api.scala._

  override def open(parameters: Configuration): Unit = {
    //    getRuntimeContext.getBroadcastVariable("table")
    //    parameters.getString(new ConfigOption[String]("table"))
    super.open(parameters)
    val driver = "com.mysql.cj.jdbc.Driver"
    Class.forName(driver).isInstance()
    conn = DriverManager.getConnection("jdbc:mysql://192.168.4.110:3306/flink", "root", "111111")
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    selectStnt = conn.prepareStatement("select * from ******")
    while (isRunning) {
      val sql_result: ResultSet = selectStnt.executeQuery
      val jbArray = mutable.ListBuffer[JsonObject]()
      while (sql_result.next()) {
        val jb = new JsonObject();
        val ids = sql_result.getString(1).trim()
        val domain = sql_result.getString(2).trim()
        jb.addProperty("", domain)
        jbArray.append(jb)
      }
      ctx.collect(jbArray.toString()) //发送结果
      jbArray.clear()
      Thread.sleep(5 * 60 * 1000)
    }
  }

  override def cancel(): Unit = {
    if (selectStnt != null) {
      selectStnt.close()
    }
    if (conn != null) {
      conn.close()
    }
    isRunning = false
  }
}

package com.wqj.flink1.sink


import java.sql.{Connection, DriverManager}

import com.wqj.flink1.pojo.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySqlSink extends RichSinkFunction[Student] {
  val jdbcUrl = "jdbc:mysql://192.168.4.110:3306/flink_test?useSSL=false&allowPublicKeyRetrieval=true"
  val username = "root"
  val password = "123456"
  val driverName = "com.mysql.jdbc.Driver"
  var conn: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(jdbcUrl, username, password)
    conn.setAutoCommit(false)
  }

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    //    val s = JSON.parseObject(value, classOf[Student])
    //    println(value)
    val p = conn.prepareStatement("insert into student(name,age) values(?,?)")
    p.setString(1, value.name)
    p.setInt(2, value.age)
    p.execute()
    conn.commit()
  }

  override def close(): Unit = {
    super.close()
    conn.close()
  }
}

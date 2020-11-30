package com.wqj.flink1.connectDim

import java.sql.{Connection, DriverManager}

import com.wqj.flink1.base.Person
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class JDBCSyncMapFunction extends RichMapFunction[Person, Person] {
  var conn: Connection = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection(
      "jdbc:mysql://192.168.4.110:3306/flink_test?characterEncoding=UTF-8&useSSL=false",
      "root",
      "123456")
  }

  override def map(value: Person): Person = {

    //根据city_id 查询 city_name
    val pst = conn.prepareStatement("select name from person where id = ?")
    pst.setInt(1, value.getId)
    val resultSet = pst.executeQuery
    var newName: String = null
    while (resultSet.next()) {
      newName = resultSet.getString(1)
    }
    pst.close
    return Person(value.id, newName, value.age)
  }

  override def close(): Unit = {
    super.close()
    if (conn != null) {
      conn.close
    }
  }
}

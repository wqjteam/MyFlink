package com.wqj.flink.base

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object InputAndOutputMysql {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val sourceData = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://192.168.4.110:3306/flink_test")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("select id,name,age,deptid from employ")
      .setRowTypeInfo(
        new RowTypeInfo(
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO,
          BasicTypeInfo.INT_TYPE_INFO))
      .finish())
    val dealdata = sourceData.map(x => {
      (x.getField(2), x.getField(3))
    })
    dealdata.print()
    val row_data = dealdata.map(record => {
      val row = new Row(2)
      row.setField(0, record._1)
      row.setField(1, record._2)

      row
    })
    row_data.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://192.168.4.110:3306/flink_test")
      .setUsername("root")
      .setPassword("123456")
      .setQuery("insert into employ_statistics(people_num,dept_id) values(?,?)")
      .finish())


    env.execute("转换数据")
  }

}

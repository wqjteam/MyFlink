package com.wqj.flink1.output

import java.util
import java.util.List

import com.wqj.flink1.base.person
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._

class FileProcessWindowFunction extends ProcessAllWindowFunction[person, person, TimeWindow] {
  val config: conf.Configuration = HBaseConfiguration.create()

  //    设置zookeeper主机名
  config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.4.110")
  //    设置端口
  config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
  //    实例化
  var conn: Connection = ConnectionFactory.createConnection(config)

  val tableName: TableName = TableName.valueOf("study:person")
  var table: Table = conn.getTable(tableName)

  override def process(context: Context, elements: Iterable[person], out: Collector[person]): Unit = {
    val family = "info"
    val puts: util.List[Put] = new util.ArrayList[Put]
    for (value <- elements) {
      val put = new Put(value.id.toString.getBytes())
      put.addColumn(family.getBytes(), "name".getBytes(), value.name.getBytes())
      put.addColumn(family.getBytes(), "age".getBytes(), value.age.toString.getBytes())
      puts.add(put)
    }

    //    mutator.mutate(put)
    table.put(puts)
    table.close()
    //    mutator.close()
    conn.close()
  }
}


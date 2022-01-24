package com.wqj.flink.intput

import com.google.gson.JsonObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan, Table, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConverters._

class HbaseReader extends RichSourceFunction[(String, String)] {
  //  创建连接
  var conn: Connection = _
  //  创建BufferedMutator
  //  作用类似put，可以实现批量异步
  var table: Table = null
  var scan: Scan = null

  override def open(parameters: Configuration): Unit = {
    val config: conf.Configuration = HBaseConfiguration.create()

    //    设置zookeeper主机名
    config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.4.110")
    //    设置端口
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    //    实例化
    conn = ConnectionFactory.createConnection(config)

    val tableName: TableName = TableName.valueOf("study:person")

    //    val params = new BufferedMutatorParams(tableName)
    //
    //    mutator = conn.getBufferedMutator(params)
    table = conn.getTable(tableName)
    scan = new Scan()
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String)]): Unit = {

    //    scan.setFilter(new Filter())
//    while (true) {
      val rs = table.getScanner(scan)

      val iterator = rs.iterator()
      while (iterator.hasNext) {
        val result = iterator.next()
        val json = new JsonObject()
        json.addProperty("id", Bytes.toString(result.getRow))
        val rowKey = Bytes.toString(result.getRow)
        val sb: StringBuffer = new StringBuffer()
        for (cell: Cell <- result.listCells().asScala) {
          val property = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          json.addProperty(property, value)
        }
        ctx.collect((rowKey, json.toString))
      }
//      Thread.sleep(50000)
//    }
  }

  override def cancel(): Unit = {
    if (table != null) {
      table.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
}

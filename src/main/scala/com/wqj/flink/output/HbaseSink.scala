package com.wqj.flink.output

import com.wqj.flink.base.Person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HbaseSink extends RichSinkFunction[Person] {
  //  创建连接
  var conn: Connection = _
  //  创建BufferedMutator
  //  作用类似put，可以实现批量异步
  var mutator: BufferedMutator = null
  var table: Table = null

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

  }

  override def invoke(value: Person, context: SinkFunction.Context[_]): Unit = {
    val family = "info"
    val put = new Put(value.id.toString.getBytes())
    put.addColumn(family.getBytes(), "name".getBytes(), value.name.getBytes())
    put.addColumn(family.getBytes(), "age".getBytes(), value.age.toString.getBytes())
    //    mutator.mutate(put)
    table.put(put)
  }

  override def close(): Unit = {
    table.close()
    //    mutator.close()
    conn.close()
  }
}
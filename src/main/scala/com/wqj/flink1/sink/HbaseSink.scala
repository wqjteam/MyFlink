package com.wqj.flink1.sink

import com.alibaba.fastjson.JSON
import com.wqj.flink1.base.person
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
class HbaseSink extends  RichSinkFunction[person] {
  //  创建连接
  var conn: Connection = _
  //  创建BufferedMutator
  //  作用类似put，可以实现批量异步
  var mutator:BufferedMutator = null

  override def open(parameters: Configuration): Unit = {
    val config: conf.Configuration = HBaseConfiguration.create()

    //    设置zookeeper主机名
    config.set(HConstants.ZOOKEEPER_QUORUM,"192.168.4.110")
    //    设置端口
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    //    实例化
    conn = ConnectionFactory.createConnection(config)

    val tableName: TableName = TableName.valueOf("person")

    val params = new BufferedMutatorParams(tableName)

    mutator = conn.getBufferedMutator(params)

  }

  override def invoke(value: person, context: SinkFunction.Context[_]): Unit = {
    val family = "info"
    val put = new Put(value.id.toString.getBytes())
    put.addColumn(family.getBytes(),"name".getBytes(),value.name.getBytes())
    put.addColumn(family.getBytes(),"age".getBytes(),value.age.toString.getBytes())
    mutator.mutate(put)
  }

  override def close(): Unit = {
    mutator.close()
    conn.close()
  }
}
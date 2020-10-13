package com.wqj.flink1.sink


import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.io.crypto.Context
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

class HbaseSink extends SinkFunction[String] {
  lazy val logger = Logger.getLogger("HbaseSink")

  @Override
  def invoke(value: String, context: Context): Unit = {
    var connection: Connection = null
    var table: Table = null
    try { // 加载HBase的配置
      val configuration = HBaseConfiguration.create
      // 读取配置文件
      configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI))
      configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI))
      connection = ConnectionFactory.createConnection(configuration)
      val tableName = TableName.valueOf("test")
      // 获取表对象
      table = connection.getTable(tableName)
      //row1:cf:a:aaa
      val split = value.split(":")
      // 创建一个put请求，用于添加数据或者更新数据
      val put = new Put(Bytes.toBytes(split(0)))
      put.addColumn(Bytes.toBytes(split(1)), Bytes.toBytes(split(2)), Bytes.toBytes(split(3)))
      table.put(put)
      logger.error("[HbaseSink] : put value:{} to hbase")
    } catch {
      case e: Exception =>
        logger.error("error:", e)
    } finally {
      if (null != table) table.close
      if (null != connection) connection.close
    }
  }
}

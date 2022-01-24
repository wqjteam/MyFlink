package com.shtd.dw.utils

import com.wqj.flink.utils.ConfUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, HBaseAdmin}

/**
  * 获取hbase的基本连接工具类
  * */
class HbaseUtils private {


  var hBaseAdmin: Admin = null
  var connection: Connection = null
  var configuration: Configuration = null
  var instance: HbaseUtils = null

  def getInstance(): HbaseUtils = {
    if (instance == null) {
      this.HbaseUtils()
      instance = this
    }
    return instance
  }

  def HbaseUtils() {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", ConfUtils.HDAOOPIP)
    //设置zookeeper连接端口，默认2181
    configuration.set("hbase.zookeeper.property.clientPort", ConfUtils.ZKPORT)
    configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT,"/hbase")

    try {
      connection = ConnectionFactory.createConnection(configuration)
      hBaseAdmin = connection.getAdmin()
    } catch {
      case e: Exception => {

        e.printStackTrace()
      }
    }


  }

}

object HbaseUtils {
  val hbaseUtils = new HbaseUtils()

  def getHbaseInstance(): HbaseUtils = {
    hbaseUtils.getInstance()
  }

  def getHbaseAdmin(): Admin = {
    getHbaseInstance.hBaseAdmin
  }

  def createTable(tableName: String): Unit = {
    val tableDescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    //    for(String f: family){
    //      tableDescriptor.addFamily(new HColumnDescriptor(f));
    //    }
    // 创建表
    getHbaseAdmin().createTable(tableDescriptor);


  }


}

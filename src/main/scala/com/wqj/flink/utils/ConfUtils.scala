package com.wqj.flink.utils

object ConfUtils {
  val HDAOOPIP = "bigdata1"
  //  val HDAOOPIP = "192.168.3.62"
  val DEFAULTDATE = "1800-01-01"
  val DEFAULTTIMESTAMPLE = "1800-01-01 00:00:00.000000"
  val DEFAULTZEROTIME = " 00:00:00.000000"
  val DEFAULTPARTITION = ""
  val HDFS = HDAOOPIP + "9820"
  val ZKPORT = "2181"
  val DATABASE = "shtd_store"
  //  val MYSQLDBURL = "jdbc:mysql://" + HDAOOPIP + ":13306/" + DATABASE + "?characterEncoding=UTF-8"
  val MYSQLDBURL = "jdbc:mysql://" + HDAOOPIP + ":3306/" + DATABASE + "?characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false"
  val MYSQLDRIVER = "com.mysql.jdbc.Driver"
  val MYSQLUSER = "root"
  val MYSQLPASSWORD = "123456"

  //kafka
  val BOOTSTRAPSERVERS = "bigdata1:9029,bigdata2:9092,bigdata3:9092"
  val GROUPID = "shtd"
  val KEYDESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"
  val VALUEDESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer"
  val AUTOOFFSET_EARLIEST = "earliest-offset"
  val ODSTOPIC = "ods_ordersall_kafka"
  val DWDTOPIC = "fact_orders"
  val DWS_TOPIC = "customer_minute_orders_kafka_aggr"


  //redis
  val REDISHOST = "bigdata1"
  val REDISPORT = 6379
  val REDISMaxTotal = 100
  val REDISTimeout = 1000 * 10


  //Hive
  private var privateHiveConfDir = "./src/main/resources"
  val DEFAULTDATABASE = "dwd"

  def HIVECONFDIR: String = {
    if (HdfsUtils.isFlock) {
      privateHiveConfDir = "/opt/module/hive-3.1.2/conf"
    } else {
      privateHiveConfDir = "./src/main/resources"
    }
    return privateHiveConfDir
  }

}

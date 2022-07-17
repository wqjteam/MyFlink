package com.wqj.flink.online.ads

import com.wqj.flink.utils.ConfUtils
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

//flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.ads.ToAdsConsumptionAmountEveryhour /sh/包名.jar yarn
//每小时销售额
//后面 flink 的建表语句都可以保存在hive的元数据中
object ToAdsConsumptionAmountEveryhour {

  def main(args: Array[String]): Unit = {

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    var tableEnv = StreamTableEnvironment.create(env)

    val configuration = tableEnv.getConfig().getConfiguration
    configuration.setString("table.exec.source.idle-timeout", "3600") //60s没有接受数据，推进watermark   （生产时根据窗口大小调整。>=窗口大小）

    val GROUPID = ConfUtils.GROUPID
    val consumeTopic = ConfUtils.DWDTOPIC

    //source： 获取kafka的 订单数据
    tableEnv.executeSql(
      s"""
         |create table source_fact_orders_kafka
         |(
         |    order_key      bigint,
         |    cust_key       bigint,
         |    order_status   string,
         |    total_price    double,
         |    order_date     timestamp,
         |    order_priority string,
         |    clerk         string,
         |    ship_priority  string,
         |    `comment`     string,
         |    create_user   string,
         |    create_time   timestamp,
         |    modify_user   string,
         |    modify_time   string,
         |    t as to_timestamp(from_unixtime(unix_timestamp(modify_time),'yyyy-MM-dd HH:mm:ss')),
         |    watermark for t as t-- - INTERVAL '5' SECOND  --指定事件时间，水印设置为5秒
         |   -- t as PROCTIME()
         |) with(
         |    'connector' = 'kafka',
         |    'topic' = '${consumeTopic}',
         |    'properties.bootstrap.servers' = '${ConfUtils.BOOTSTRAPSERVERS}',
         |    'properties.group.id' = '${GROUPID}',
         |    'scan.startup.mode' = '${ConfUtils.AUTOOFFSET_EARLIEST}',
         |    'format' = 'json'
         |    )
         |""".stripMargin)
    val result = tableEnv.executeSql("select * from source_fact_orders_kafka")
    val table: Table = tableEnv.sqlQuery("select * from source_fact_orders_kafka")

    val resultTable: Table = tableEnv.sqlQuery(
      s"""
         |select TUMBLE_START(t,INTERVAL '1' hour) as wStart,
         |       TUMBLE_END(t,INTERVAL '1' hour) as wEnd,
         |       sum(total_price)
         |from source_fact_orders_kafka
         |group by TUMBLE(t,INTERVAL '1' hour)
         |""".stripMargin)


    //    val resultTable = tableEnv.sqlQuery(
    //      s"""
    //         |SELECT window_start, window_end, SUM(total_price)
    //         |  FROM TABLE(
    //         |    TUMBLE(TABLE source_fact_orders_kafka, DESCRIPTOR(t), INTERVAL '10' MINUTES))
    //         |  GROUP BY window_start, window_end
    //         |""".stripMargin)

    val resultDS = tableEnv.toDataStream(resultTable)
    resultDS.print()
    val tResult = resultDS.map(rds => {
      (rds.getField(0).toString, rds.getField(2).toString)//（窗口开始时间，每小时销售额）
    })

    val redisConf = new FlinkJedisPoolConfig.Builder()
      .setHost(ConfUtils.REDISHOST)
      .setPort(ConfUtils.REDISPORT)
      .setMaxTotal(ConfUtils.REDISMaxTotal)
      .setTimeout(ConfUtils.REDISTimeout)
      .build()
    tResult.addSink(new RedisSink[(String, String)](redisConf, new MyRedisSinkMapperForHour))


    env.execute("Ads_Consumption_Amount_Everyhour")
  }


}

class MyRedisSinkMapperForHour extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}

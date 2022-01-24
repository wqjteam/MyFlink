package com.wqj.flink.other.online.ads

import com.shtd.dw.bean.CustomerMinuteOders
import com.shtd.dw.utils.ConfUtils
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

//flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.ads.ToAdsOrderedAmountEveryminute /sh/包名.jar yarn
//每分钟订单量
object ToAdsOrderedAmountEveryminute {

  def main(args: Array[String]): Unit = {

    var env = StreamExecutionEnvironment.getExecutionEnvironment
    var tableEnv = StreamTableEnvironment.create(env)

    //    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
    //      val configuration: Configuration = new Configuration()
    //      configuration.setString(RestOptions.BIND_PORT, "8081-8089")
    //      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    //      tableEnv = StreamTableEnvironment.create(env)
    //    }

//    env.setParallelism(6)
//    env.enableCheckpointing(50000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val GROUPID = ConfUtils.GROUPID
    val DWS_TOPIC = ConfUtils.DWS_TOPIC

    //source：
    tableEnv.executeSql(
      s"""
         |create table source_customer_minute_orders__kafka_aggr
         |(
         |    cust_key         String,
         |    total_consumption double,
         |    total_order       int,
         |    `year`            String,
         |    `month`            String,
         |    `day`              String,
         |    `hour`             String,
         |    `minute`           String,
         |    dws_insert_time  String,
         |    dws_update_time  String--,
         |   -- t as to_timestamp(from_unixtime(unix_timestamp(modify_time),'yyyy-MM-dd HH:mm:ss')),
         |    --watermark for t as t -- - INTERVAL '5' SECOND  --指定事件时间，水印设置为5秒
         |) with(
         |    'connector' = 'kafka',
         |    'topic' = '${DWS_TOPIC}',
         |    'properties.bootstrap.servers' = '${ConfUtils.BOOTSTRAPSERVERS}',
         |    'properties.group.id' = '${GROUPID}',
         |    'scan.startup.mode' = '${ConfUtils.AUTOOFFSET_EARLIEST}',
         |    'format' = 'json'
         |    )
         |""".stripMargin)
    val table = tableEnv.sqlQuery(s"select * from source_customer_minute_orders__kafka_aggr")
    val kadkaDS = tableEnv.toDataStream(table)
    val timeWithCountDS = kadkaDS.map(data => {
      CustomerMinuteOders(
        data.getField(2).toString.toInt
        , data.getField(3).toString + "-"
          + data.getField(4).toString + "-"
          + data.getField(5).toString + " "
          + data.getField(6).toString + ":"
          + data.getField(6).toString
      )
    })
    val resultDS: DataStream[CustomerMinuteOders] = timeWithCountDS.keyBy(_.order_date)
      .sum("total_order")
    resultDS.print()

    val redisConf = new FlinkJedisPoolConfig.Builder()
      .setHost(ConfUtils.REDISHOST)
      .setPort(ConfUtils.REDISPORT)
      .setMaxTotal(ConfUtils.REDISMaxTotal)
      .setTimeout(ConfUtils.REDISTimeout)
      .build()
    resultDS.addSink(new RedisSink[CustomerMinuteOders](redisConf, new RedisHashMapper))

    env.execute("Ads_Ordered_Amount_Everyminute ")
  }


}

class RedisHashMapper extends RedisMapper[CustomerMinuteOders] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "ordered_amount_everyminute")
  }

  override def getKeyFromData(data: CustomerMinuteOders): String = {
    data.order_date
  }

  override def getValueFromData(data: CustomerMinuteOders): String = {
    data.total_order.toString
  }
}

package com.wqj.flink.online.ads


import java.util.HashMap

import com.wqj.flink.utils.ConfUtils
import com.wqj.flink.utils.udf.FlinkMedianUdaf
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row


//flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.ads.ToAdsTop5MidContry /sh/包名.jar yarn
//消费额中位数前5的国家

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object ToAdsTop5MidContry {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      val configuration: Configuration = new Configuration()
      configuration.setString("rest.bind-port", "8083")
      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
      tableEnv = StreamTableEnvironment.create(env)
    }
    val name = "myhive"
    val hive = new HiveCatalog(name, ConfUtils.DEFAULTDATABASE, ConfUtils.HIVECONFDIR)
    tableEnv.registerCatalog(name, hive)
    tableEnv.useCatalog(name)
    tableEnv.getConfig().getConfiguration.setString("table.exec.source.idle-timeout", "10")
    //注册自定义函数
    tableEnv.registerFunction("median", new FlinkMedianUdaf())

    tableEnv.executeSql(
      s"""
         |CREATE TABLE IF not EXISTS  dwdorder (
         |order_key BIGINT,
         |cust_key BIGINT,
         |order_status String,
         |total_price DOUBLE,
         |order_date TIMESTAMP(14),
         |order_priority String,
         |clerk String,
         |ship_priority String,
         |`comment` String,
         |create_user String,
         |create_time TIMESTAMP(14),
         |modify_user String,
         |modify_time String,
         | t as to_timestamp(from_unixtime(unix_timestamp(modify_time),'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - INTERVAL '5' SECOND,
         |proctime as PROCTIME()
         |) WITH (
         | 'connector' = 'kafka',
         | 'topic' = '${ConfUtils.DWDTOPIC}',
         | 'properties.bootstrap.servers' = '${ConfUtils.BOOTSTRAPSERVERS}',
         | 'properties.group.id' = '${ConfUtils.GROUPID + "midcountry"}',
         | 'format' = 'json',
         | 'scan.startup.mode' = 'earliest-offset'
         |)
         |
        |
      """.stripMargin)
    tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true");

    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
    val order_nation = tableEnv.sqlQuery(
      """
        |
        |select fact.order_key,fact.total_price,fact.cust_key,dim_cu.nation_key,dim_na.name,t
        | from dwdorder as fact
        |left JOIN myhive.dwd.dim_customer
        | /*+OPTIONS('streaming-source.enable' = 'true',
        |  'streaming-source.partition.include' = 'latest',
        | 'streaming-source.monitor-interval' = '1h',
        |  'streaming-source.partition-order' = 'partition-name'
        |
        |  )*/
        |FOR SYSTEM_TIME AS OF fact.proctime AS dim_cu
        |on fact.cust_key=dim_cu.cust_key
        |
        |left join myhive.dwd.dim_nation
        | /*+OPTIONS('streaming-source.enable' = 'true',
        |  'streaming-source.partition.include' = 'latest',
        | 'streaming-source.monitor-interval' = '1h',
        |  'streaming-source.partition-order' = 'partition-name'
        |   )*/
        |  FOR SYSTEM_TIME AS OF fact.proctime AS dim_na
        |on dim_cu.nation_key=dim_na.nation_key
        |
        |
      """.stripMargin)
    //      .printSchema()
    //    tableEnv.toRetractStream[Row](order_nation).print()
    tableEnv.createTemporaryView("order_nation", order_nation)
    //        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE)

    //        val a=tableEnv.sqlQuery(
    //      """
    //        |select
    //        |TUMBLE_START(t,INTERVAL '1' hour),
    //        |TUMBLE_END(t,INTERVAL '1' hour),
    //        |row_number() over(partition by TUMBLE_START(t,INTERVAL '1' hour) , TUMBLE_END(t,INTERVAL '1' hour) order by  total_price) as num,
    //        | count(cust_key) as cun
    //        | from
    //        |order_nation
    //        |group by nation_key, TUMBLE(t,INTERVAL '1' hour)
    //        |
    //        |
    //          """.stripMargin)


    //    val a = tableEnv.sqlQuery(
    //      """
    //        |SELECT *
    //        |FROM (
    //        |
    //        |select nation_key,window_start,window_end,all_count,total_price
    //        |      ,ROW_NUMBER() OVER (PARTITION BY window_start, window_end,nation_key ORDER BY total_price DESC) as rownum
    //        |      from(
    //        |   select
    //        |       nation_key,window_start,window_end
    //        |       ,count(*) as all_count
    //        |       --,LISTAGG(cast(total_price as String)) as total_price2
    //        |       ,collect(total_price) as total_price2
    //        |     from TABLE(TUMBLE(TABLE order_nation,DESCRIPTOR(t),INTERVAL '10' MINUTES))
    //        |      GROUP BY nation_key,window_start, window_end
    //        |       )a ,UNNEST(total_price2) as t(total_price)
    //        |) WHERE rownum < 5
    //      """.stripMargin)

    val a = tableEnv.sqlQuery(
      """
        |
        |select
        |window_start
        |,COLLECT(CONCAT_WS('&&',
        |cast(rownum as String),cast(nation_key as String),name,cast(median as String) )) as  data
        |from(
        |   select
        |   median
        |   ,nation_key
        |   ,name
        |   ,ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY median asc) as rownum
        |   ,window_start
        |   from (
        |       select
        |       nation_key,
        |       max(name) as name,
        |       median(total_price) as median,
        |       TUMBLE_START(t,INTERVAL '1' hour) as window_start,
        |       TUMBLE_END(t,INTERVAL '1' hour) as window_end
        |         from
        |       order_nation
        |       group by nation_key, TUMBLE(t,INTERVAL '1' hour)
        |    )
        |)b where b.rownum<=5 group by b.window_start
      """.stripMargin)


    val redisConf = new FlinkJedisPoolConfig.Builder()
      .setHost(ConfUtils.REDISHOST)
      .setPort(ConfUtils.REDISPORT)
      .setMaxTotal(ConfUtils.REDISMaxTotal)
      .setTimeout(ConfUtils.REDISTimeout)
      .build()

    val redisdata = tableEnv.toRetractStream[Row](a).map(x => {
      val returnmap = new HashMap[Int, String]()
      val keyarry = x._2.getFieldAs[HashMap[Int, String]]("data").keySet().toArray()
      keyarry.foreach(keyset => {

        val splidata = keyset.toString.split("&&")
        returnmap.put(splidata(0).toInt, splidata.slice(1, splidata.size).mkString("||"))

      })
      returnmap.toString()

    })

    redisdata.addSink(new RedisSink[String](redisConf, new ToAdsTop5MidContryRedisMapper()))


    env.execute("ToAdsTop5MidContry")
  }
}


class ToAdsTop5MidContryRedisMapper extends RedisMapper[String] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(t: String): String = {
    "top5_mid_country"
  }

  override def getValueFromData(t: String): String = {
    t
  }
}

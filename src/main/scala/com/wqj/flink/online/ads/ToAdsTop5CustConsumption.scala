package com.wqj.flink.online.ads

import java.{lang, util}

import com.wqj.flink.bean.SourceKafkaDwd
import com.wqj.flink.utils.{ConfUtils, TimeTransform}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


//per-job模式
// flink run –m yarn-cluster -c 全类名 .jar
//flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.ads.ToAdsTop5CustConsumption /sh/包名.jar yarn
//统计消费额前5的用户
//后面 flink 的建表语句都可以保存在hive的元数据中
object ToAdsTop5CustConsumption {

  def main(args: Array[String]): Unit = {


    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //        if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
    //          val configuration: Configuration = new Configuration()
    //          configuration.setString(RestOptions.BIND_PORT, "8081") //"8081-8089"
    //          env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    //          tableEnv = StreamTableEnvironment.create(env)
    //        }

//        env.setParallelism(6)
//        env.enableCheckpointing(50000)
//        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val GROUPID = ConfUtils.GROUPID
    val consumeTopic = ConfUtils.DWDTOPIC
    val DWS_TOPIC = ""

    val name = "myhive"
    val hive = new HiveCatalog(name, ConfUtils.DEFAULTDATABASE, ConfUtils.HIVECONFDIR)
    tableEnv.registerCatalog(name, hive)
    tableEnv.useCatalog(name)


    //source： 获取kafka的 订单数据
//    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
//第一次运行后，表结构等信息已经存储在hive中。（运行一次后可注掉）

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
         |    modify_time   string--,
         |   -- t as to_timestamp(from_unixtime(unix_timestamp(modify_time),'yyyy-MM-dd HH:mm:ss')),
         |    --watermark for t as t -- - INTERVAL '5' SECOND  --指定事件时间，水印设置为5秒
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
    val dwdOders: Table = tableEnv.sqlQuery("select *,PROCTIME() AS proctime from source_fact_orders_kafka")
    tableEnv.createTemporaryView("dwdOrder",dwdOders)
    tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled", "true")
    //table.printSchema()


//    tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT)
    //关联hive的维表 获得姓名字段
    val table: Table = tableEnv.sqlQuery(
      s"""
         |select
         |    order_key,
         |    factoder.cust_key,
         |    order_status,
         |    total_price,
         |    order_date,
         |    order_priority,
         |    clerk,
         |    ship_priority,
         |    factoder.`comment`,
         |    factoder.create_user,
         |    factoder.create_time,
         |    factoder.modify_user,
         |    factoder.modify_time,
         |     dim.name
         |from dwdOrder as factoder
         |left join  myhive.dwd.dim_customer
         |/*+OPTIONS('streaming-source.enable' = 'true',
         |  'streaming-source.partition.include' = 'latest',
         | 'streaming-source.monitor-interval' = '1h',
         |  'streaming-source.partition-order' = 'partition-name',
         |  'lookup.join.cache.ttl' = '12 h')*/
         |  FOR SYSTEM_TIME AS OF factoder.proctime as dim
         |on factoder.cust_key=dim.cust_key
         |""".stripMargin)



    val kafkaDS = tableEnv.toRetractStream[Row](table)
    val beanDS: DataStream[SourceKafkaDwd] = kafkaDS.map(rows => {
      SourceKafkaDwd(
        rows._2.getField(0).toString
        ,  rows._2.getField(1).toString
        , rows._2.getField(13).toString
        ,  rows._2.getField(2).toString
        ,  rows._2.getField(3).toString.toDouble
        ,  rows._2.getField(4).toString
        ,  rows._2.getField(5).toString
        ,  rows._2.getField(6).toString
        ,  rows._2.getField(7).toString
        ,  rows._2.getField(8).toString
        ,  rows._2.getField(9).toString
        ,  rows._2.getField(10).toString
        ,  rows._2.getField(11).toString
        , TimeTransform.tranTimeToLong( rows._2.getField(12).toString)
      )
    })

    val custWithTotalPriceDS: DataStream[TopBean] = beanDS.keyBy(_.cust_key)
      .process(new MyKeydedProcess()) //每人截止当前时间的消费总额 （用户key，消费总额）

    val top5ResultDS: DataStream[List[TopBean]] = custWithTotalPriceDS.keyBy(_.process_key).process(new MyProcessFunctionForResult()) //.setParallelism(1)
    top5ResultDS.print()

    //sink： 写入Redis
    // 连接到Redis的配置
    val redisConf = new FlinkJedisPoolConfig.Builder()
      .setHost(ConfUtils.REDISHOST)
      .setPort(ConfUtils.REDISPORT)
      .setMaxTotal(ConfUtils.REDISMaxTotal)
      .setTimeout(ConfUtils.REDISTimeout)
      .build()
    top5ResultDS.addSink(new RedisSink[List[TopBean]](redisConf, new MyRedisMapper()))

    env.execute("top5_cust_consumption")
  }


  class MyKeydedProcess() extends KeyedProcessFunction[String, SourceKafkaDwd, TopBean] {

    private var totalPrice: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
      totalPrice = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("topnKeyby", classOf[Double]))
    }

    override def processElement(value: SourceKafkaDwd, ctx: KeyedProcessFunction[String, SourceKafkaDwd, TopBean]#Context, out: Collector[TopBean]): Unit = {
      //获取状态
      val total: Double = totalPrice.value()
      //累加
      var newTotal: Double = 0.0d
      newTotal = total + value.total_price
      //更新状态
      totalPrice.update(newTotal)
      val cutkey = ctx.getCurrentKey
      val custname: String = value.cust_name
      out.collect(TopBean("top5_cust", cutkey + "||" + custname, newTotal))
    }
  }


  /*
  class MyProcessFunction() extends ProcessFunction[TopBean, List[TopBean]] {

    private var topN: ListState[TopBean] = _

    override def open(parameters: Configuration): Unit = {
      val context: RuntimeContext = getRuntimeContext
      topN = context.getListState[TopBean](new ListStateDescriptor[TopBean]("topN", classOf[TopBean]))
    }

    override def processElement(tt: TopBean, ctx: ProcessFunction[TopBean, List[TopBean]]#Context, out: Collector[List[TopBean]]): Unit = {

      //-----测试日志
      //      println("输入值：" + tt.total_price)

      var result: List[TopBean] = List() //存储 状态中所有值
      var top5: List[TopBean] = List() //存储 前五
      // 1.将新到数据添加在状态中
      topN.add(tt)
      // 2.对状态中数据排序
      val beans: lang.Iterable[TopBean] = topN.get()
      val topnIt: util.Iterator[TopBean] = beans.iterator() //获取迭代器
      while (topnIt.hasNext) {
        val topNB = topnIt.next()
        topNB +: result

        //-----测试日志
        //        println("topNB:" + topNB)
      }

      val sortResult: List[TopBean] = result.sortBy(_.total_price)(Ordering.Double.reverse) //从大到小排序（默认是从小到大）
      //取前五
      for (i <- 0 to 5) {
        sortResult.take(i) +: top5
      }
      //-----测试日志 start
      //      val top5iter = top5.iterator
      //      while (top5iter.hasNext) {
      //        val bean: TopBean = top5iter.next()
      //        println("top5:" + bean)
      //      }
      //----- 测试日志 end
      out.collect(top5)
    }
  }

   */

  class MyProcessFunctionForResult() extends KeyedProcessFunction[String, TopBean, List[TopBean]] {

    private var topN: ListState[TopBean] = _

    override def open(parameters: Configuration): Unit = {
      val context: RuntimeContext = getRuntimeContext
      topN = context.getListState[TopBean](new ListStateDescriptor[TopBean]("topN", classOf[TopBean]))
    }

    override def processElement(tt: TopBean, ctx: KeyedProcessFunction[String, TopBean, List[TopBean]]#Context, out: Collector[List[TopBean]]): Unit = {
      //-----测试日志
      //println("输入值：" + tt.total_price)
      var result = new ListBuffer[TopBean] //存储 状态中所有值
      var top5 = new ListBuffer[TopBean] //存储 前五
      // 1.将新到数据添加在状态中
      topN.add(tt)
      // 2.对状态中数据排序
      val beans: lang.Iterable[TopBean] = topN.get()
      val topnIt: util.Iterator[TopBean] = beans.iterator() //获取迭代器
      while (topnIt.hasNext) {
        val topNB: TopBean = topnIt.next()
        //存储
        result += topNB
        //-----测试日志
        //println("topNB:" + topNB)
      }
      val sortResult: ListBuffer[TopBean] = result.sortBy(_.total_price)(Ordering.Double.reverse)
      //从大到小排序（默认是从小到大）
      //取前五
      for (i: Int <- 0 to 4) {
        if (i < sortResult.size) {
          val bean: TopBean = sortResult(i)
          top5 += bean
        }
      }
      //-----测试日志 start
//      val top5iter = top5.iterator
//      while (top5iter.hasNext) {
//        val bean: TopBean = top5iter.next()
//        println("top5:" + bean)
//      }
      //----- 测试日志 end
      out.collect(top5.toList)
    }
  }

  class MyRedisMapper extends RedisMapper[List[TopBean]] {

    override def getCommandDescription: RedisCommandDescription = {
      //new RedisCommandDescription(RedisCommand.HSET, "top5_cust_consumption")
      new RedisCommandDescription(RedisCommand.SET)
    }

    override def getKeyFromData(data: List[TopBean]): String = {
      "top5_cust_consumption"
    }

    override def getValueFromData(data: List[TopBean]): String = {
      val iterator = data.iterator
      val builder = new StringBuilder
      while (iterator.hasNext) {
        val value = iterator.next()
        builder.append("==" + value + "==") //"\r\n"
      }
      builder.toString()
    }
  }

}


case class TopBean(
                    process_key: String,
                    cust_key: String,
                    total_price: Double
                  )



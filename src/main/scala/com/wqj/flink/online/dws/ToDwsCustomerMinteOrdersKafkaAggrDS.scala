package com.wqj.flink.online.dws

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import com.wqj.flink.bean.{SourceKafkaDwdNameL, WindowAggrKafka}
import com.wqj.flink.utils.{ConfUtils, TimeTransform}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


//kafka-topics.sh --create --zookeeper bigdata1:2181/kafka --replication-factor 1 --partitions 2 --topic customer_minute_orders__kafka_aggr
//per-job模式
// flink run –m yarn-cluster -c 全类名 .jar

//flink run -m yarn-cluster -p 2 -yjm 2G -ytm 2G -yn -c com.shtd.dw.online.dws.ToDwsCustomerMinteOrdersKafkaAggrDS /sh/包名.jar yarn


object ToDwsCustomerMinteOrdersKafkaAggrDS {


  def main(args: Array[String]): Unit = {

    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var tabaleEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
    //      val configuration: Configuration=new Configuration()
    //      configuration.setString("rest.bind-port", "12233")
    //      env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration)
    //      tabaleEnv = StreamTableEnvironment.create(env)
    //    }


//    env.setParallelism(6)
//    env.enableCheckpointing(50000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    val GROUPID = ConfUtils.GROUPID
    val consumeTopic = ConfUtils.DWDTOPIC
    val DWS_TOPIC = ConfUtils.DWS_TOPIC


    // 1.source：kafka
        tabaleEnv.executeSql(
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
         |    --t as to_timestamp(from_unixtime(unix_timestamp(modify_time),'yyyy-MM-dd HH:mm:ss')),
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
    //        val result = tabaleEnv.executeSql("select * from source_fact_orders_kafka")
    //        result.print()
    val table = tabaleEnv.sqlQuery(s"select * from source_fact_orders_kafka")
//    val table: Table = tabaleEnv.from("source_fact_orders_kafka")
//    table.printSchema()

//    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //表转流
    val stream = tabaleEnv.toRetractStream[Row](table).map(rows => {
      SourceKafkaDwdNameL(rows._2.getField(0).toString
        , rows._2.getField(1).toString
        , rows._2.getField(2).toString
        , rows._2.getField(3).toString.toDouble
        , rows._2.getField(4).toString
        , rows._2.getField(5).toString
        , rows._2.getField(6).toString
        , rows._2.getField(7).toString
        , rows._2.getField(8).toString
        , rows._2.getField(9).toString
        , rows._2.getField(10).toString
        , rows._2.getField(11).toString
//        , fm.parse(rows._2.getField(12).toString).getTime
        ,TimeTransform.tranTimeToLong(rows._2.getField(12).toString)
      )
    })
    stream.print()



/*
    val source = KafkaSource.builder()
      .setBootstrapServers(ConfUtils.BOOTSTRAPSERVERS)
      .setTopics(ConfUtils.DWDTOPIC)
      .setGroupId(ConfUtils.GROUPID)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
    val kafkaDataStream: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
    //    kafkaDataStream.print()

    val stream = kafkaDataStream.map(rows => {
      val nObject: JSONObject = JSON.parseObject(rows)

      val order_key = nObject.getLong("order_key").toString
      val cust_key = nObject.getLong("cust_key").toString
      val order_status = nObject.getString("order_status")
      val total_price = nObject.getDouble("total_price")
      val order_date = nObject.getString("order_date")
      val order_priority = nObject.getString("order_priority")
      val clerk = nObject.getString("clerk")
      val ship_priority = nObject.getString("ship_priority")
      val comment = nObject.getString("comment")
      val create_user = nObject.getString("create_user")
      val create_time = nObject.getString("create_time")
      val modify_user = nObject.getString("modify_user")
      val modify_time = tranTimeToLong(nObject.getString("modify_time"))

      SourceKafkaDwdNameL(
        order_key,
        cust_key,
        order_status,
        total_price,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment,
        create_user,
        create_time,
        modify_user,
        modify_time
      )
    })
//    stream.print()
*/

    //设定事件时间及水位线
    val sourceKafkaWithWatermarkDstream = stream
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[SourceKafkaDwdNameL](Duration.ofSeconds(1))
        .withTimestampAssigner(new SerializableTimestampAssigner[SourceKafkaDwdNameL] {
          override def extractTimestamp(element: SourceKafkaDwdNameL, recordTimestamp: Long): Long = element.modify_time
        }).withIdleness(Duration.ofSeconds(60)))



    val keyDS = sourceKafkaWithWatermarkDstream.keyBy(_.cust_key)

    val winDS = keyDS.window(TumblingEventTimeWindows.of(Time.seconds(60)))


    // 客户每分钟消费计算结果流
    val aggKafkaResult: DataStream[WindowAggrKafka] = winDS.process(new MyWindowProcesFunction())
    //    aggKafkaResult.print()
    tabaleEnv.createTemporaryView("aggKafkaResult", aggKafkaResult)

    //sink
    tabaleEnv.executeSql(
      s"""
         |create table sink_customer_minute_orders__kafka_aggr
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
         |    dws_update_time  String
         |)with(
         |    'connector' = 'kafka',
         |    'topic' = '${DWS_TOPIC}',
         |    'properties.bootstrap.servers' = '${ConfUtils.BOOTSTRAPSERVERS}',
         |    'format' = 'json'
         |)
         |""".stripMargin)


    tabaleEnv.executeSql(
      s"""
         |insert into sink_customer_minute_orders__kafka_aggr
         |select * from aggKafkaResult
         |""".stripMargin)

    env.execute("DWS_CustomerMinteOrdersKafkaAggrDS")


  }

  class MyWindowProcesFunction() extends ProcessWindowFunction[SourceKafkaDwdNameL, WindowAggrKafka, String, TimeWindow] {

//    def tranTimeToString(tm: Long): String = {
//      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//      val tim = fm.format(new Date(tm))
//      tim
//    }
    override def process(key: String, context: Context, elements: Iterable[SourceKafkaDwdNameL], out: Collector[WindowAggrKafka]): Unit = {

      var total_consumption: Double = 0
      val total_order = elements.size
      elements.foreach(ele =>
        total_consumption = ele.total_price + total_consumption
      )
      val winDate: String = TimeTransform.tranTimeToString(context.currentWatermark)
      val year = winDate.substring(0, 4)
      val month = winDate.substring(5, 7)
      val day = winDate.substring(8, 10)
      val hour = winDate.substring(11, 13)
      val minute = winDate.substring(14, 16)

      val sdf = new SimpleDateFormat("yyyyMMdd")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
      val time = sdf2.format(new Date())

      out
        .collect(WindowAggrKafka(key, total_consumption, total_order, year, month, day, hour, minute, time, time))
    }
  }


}

package com.wqj.flink1.base

import java.text.SimpleDateFormat
import java.util.Properties
import java.util

import com.google.gson.Gson
import com.wqj.flink1.intput.HbaseReader
import com.wqj.flink1.output.{ElasticSearchSink, FileProcessWindowFunction, HbaseSink, RedisExampleMapper}
import com.wqj.flink1.pojo.RedisBasePojo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.http.HttpHost
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

object OperateHbase {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc2"
  private val topic = "flink_test"
  val properties = new Properties()
  properties.setProperty("zookeeper.connect", zk)
  properties.setProperty("bootstrap.servers", broker)
  properties.setProperty("group.id", group_id)

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val hive = new HiveCatalog("myhive", "study", "D:\\develop_disk\\java\\MyFlink\\src\\main\\resource", "2.3.4")
    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")
    tableEnv.useDatabase("study")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)


    /**
      * kafka的consumer，flink_test是要消费的topic
      **/
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      Person(field(0).toInt, field(1), field(2).toInt)
    })
      //解决数据乱序到达的问题
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Person](Time.milliseconds(50)) {
//      override def extractTimestamp(element: Person): Long = {
//        val sdf = new SimpleDateFormat()
//        //              println("want watermark : " + sdf.parse(element.createTime).getTime)
//        //              sdf.parse(element.createTime).getTime
//          从数据中获取数据设置eventtime
//        100000l
//      }
//    })
    tableEnv.createTemporaryView("StreamPerson", stream)
    val kafkaAndHive = tableEnv.sqlQuery("select * from StreamPerson union select * from study.person")
    val streamresult = tableEnv.toRetractStream[Person](kafkaAndHive)

    /**
      * hbase数据导入
      **/
    val hbasesource = env.addSource(new HbaseReader).map(tp => {
      //      Person(field(0).toInt, field(1), field(2).toInt)
      new Gson().fromJson(tp._2, classOf[Person])
    })
    tableEnv.createTemporaryView("hbasePerson", hbasesource)
    val hbseAndkafkaResult = tableEnv.sqlQuery("select * from hbasePerson union select * from StreamPerson")
    val adkkr = tableEnv.toRetractStream[Person](hbseAndkafkaResult)
    adkkr.print()

    /**
      * hbase
      **/
    //    streamresult.map(P => P._2).addSink(new HbaseSink).name("hbasesink")

    /**
      * redis
      **/
    val redisS = streamresult.map(P => {
      new RedisBasePojo(P._2.id.toString, new Gson().toJson(P._2))
    })

    val conf = new FlinkJedisPoolConfig.Builder().setHost("flinkmaster").setPort(6379).build()
    val redisSink = new RedisSink[RedisBasePojo](conf, new RedisExampleMapper)
    //    redisS.addSink(redisSink).name("redisSink")

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))
    //    val esSinkBuild = new ElasticsearchSink.Builder[Person](httpHosts, new ElasticSearchSink())
    //    adkkr.map(x => x._2).addSink(esSinkBuild.build())

    /**
      *
      * 使用window窗口
      **/
    streamresult.map(pe => {
      new Gson().toJson(pe)
    }).setParallelism(1)
      .timeWindowAll(Time.seconds(2))
      /**
        * allowedLateness 用来控制窗口的销毁时间，解决窗口触发后数据迟到后的问题
        * 默认情况下，当watermark通过end-of-window激活window计算结束之后，再有之前的数据到达时，这些数据会被删除。
        * 为了避免有些迟到的数据被删除，因此产生了allowedLateness，使用allowedLateness延迟销毁窗口，
        * 允许有一段时间（也是以event time来衡量）来等待之前的数据到达，以便再次处理这些数据
        *
        * 同时还有网络等原因数据延迟造成 allowedLateness解决不了可以用getSideOutput(outputTag)解决
        * */

//    .allowedLateness(Time.seconds(2))
      //      .sideOutputLateData("")
    .process(new FileProcessWindowFunction()).name("possessink").print()
    env.execute("OperateHbase")

  }
}

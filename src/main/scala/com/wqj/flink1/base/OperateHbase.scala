package com.wqj.flink1.base

import java.util.Properties

import com.google.gson.Gson
import com.wqj.flink1.output.{FileProcessWindowFunction, HbaseSink, RedisExampleMapper}
import com.wqj.flink1.pojo.RedisBasePojo
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._

object OperateHbase {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc2"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val hive = new HiveCatalog("myhive", "study", "D:\\develop_disk\\java\\MyFlink\\src\\main\\resource", "2.3.4")
    tableEnv.registerCatalog("myhive", hive)
    tableEnv.useCatalog("myhive")
    tableEnv.useDatabase("study")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    //kafka的consumer，test1是要消费的topic

    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      Person(field(0).toInt, field(1), field(2).toInt)
    })
    //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor)
    tableEnv.createTemporaryView("StreamPerson", tableEnv.fromDataStream(stream))
    val kafkastream = tableEnv.sqlQuery("select * from StreamPerson union select * from study.person")
    val streamresult = tableEnv.toRetractStream[Person](kafkastream)

    /**
      * hbase
      **/
    streamresult.map(P=>P._2).addSink(new HbaseSink).name("hbasesink")
    /**
      * redis
      **/
    val redisS = streamresult.map(P => {
      new RedisBasePojo(P._2.id.toString, new Gson().toJson(P._2))
    })

    val conf = new FlinkJedisPoolConfig.Builder().setHost("flinkmaster").setPort(6379).build()
    val redisSink = new RedisSink[RedisBasePojo](conf, new RedisExampleMapper)
    //    redisS.addSink(redisSink).name("redisSink")
    /**
      * hive
      **/
    streamresult.map(pe => {
      new Gson().toJson(pe)
    }).setParallelism(1)
      .timeWindowAll(Time.seconds(2)).process(new FileProcessWindowFunction()).name("possessink").print()
    env.execute("OperateHbase")
  }
}

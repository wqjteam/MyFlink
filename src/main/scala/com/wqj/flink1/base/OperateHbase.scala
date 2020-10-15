package com.wqj.flink1.base

import java.lang
import java.util.Properties

import com.google.gson.Gson
import com.wqj.flink1.pojo.{PersonJ, RedisBasePojo}
import com.wqj.flink1.output.{FileProcessWindowFunction, HbaseSink, RedisExampleMapper}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object OperateHbase {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc2"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    //    tableEnv.connect(new Kafka().version("0.11").topic("test2").property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092").property("zookeeper.connect","hadoop102:2181")
    //      .property(ConsumerConfig.GROUP_ID_CONFIG,"ae"))
    //      .withFormat(new Csv().fieldDelimiter(' '))
    //      .withSchema(new Schema().field("name",DataTypes.STRING())
    //        .field("age",DataTypes.INT()))
    //      .createTemporaryTable("kafkaInputTable")
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    //kafka的consumer，test1是要消费的topic
    import org.apache.flink.api.scala._
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      Person(field(0).toInt, field(1), field(2).toInt)
    })
    //      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor)
    tableEnv.createTemporaryView("person", tableEnv.fromDataStream(stream))
    val kafkastream = tableEnv.sqlQuery("select * from person")
    val streamresult = tableEnv.toAppendStream[Person](kafkastream)

    /**
      * hbase
      **/
    //    streamresult.addSink(new HbaseSink).name("hbasesink")
    /**
      * redis
      **/
    val redisS = streamresult.map(person => {
      new RedisBasePojo(person.id.toString, new Gson().toJson(person))
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

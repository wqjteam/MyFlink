package com.wqj.flink1.base

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.log4j.Logger

/**
  * @Auther: wqj
  * @Date: 2019/2/17 14:32
  * @Description:
  */
object WcKafka {
  private val zk = "hadoop1:2181"
  private val broker = "hadoop1:9092"
  private val group_id = "wc1"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    lazy val logger = Logger.getLogger(WcKafka.getClass)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    val consumer = new FlinkKafkaConsumer011(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(consumer).map(x => {
      x
    })
    val wordcount = stream
      .flatMap(_.split(" "))
      .filter(x => x != null)
      .map((_, 1))
      /**
        * 相当于groupby第一个字段
        * */
      .keyBy(0)
      .sum(1)
      /**
        * 设置并发度,相当于repartiion或者coalesce
        * */
      .setParallelism(2)
      .map(x => (x._1, x._2.toString))/**返回为tuple*/
    /**
      * 设置redis的端口
      * */
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop1").setPort(6379).build()
    val sink = new RedisSink[(String, String)](conf, new RedisExampleMapper)
    wordcount.print()
    wordcount.addSink(sink)
    /**
      * 开始执行,相当于streaming 的action算子
      * 为显示触发
      * */
    env.execute("flink streaming")
  }
}

/**
  * 在flink on yarn集群中直接写入redis
  * */
class RedisExampleMapper extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  /**
    * 该方法可以直接通过.map(_=>{_.1})或者.map(_=>{_.2})提取
    * */
  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2


}

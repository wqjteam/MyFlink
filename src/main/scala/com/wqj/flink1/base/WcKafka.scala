package com.wqj.flink1.base

import java.util.{Collections, Properties}

import com.wqj.flink1.sink.{MySqlSink, RedisExampleMapper}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.log4j.Logger

/**
  * @Auther: wqj
  * @Date: 2019/2/17 14:32
  * @Description:
  */
object WcKafka {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
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
    //kafka的consumer，test1是要消费的topic
    val kafkaSource=new FlinkKafkaConsumer( topic,new SimpleStringSchema,properties)
    val stream = env.addSource(kafkaSource).map(x => {
      x
    })
    val wordcount = stream
      .flatMap(_.split(" "))
      .filter(x => x != null)
      .map((_, 1))

      /**
        * 相当于groupby第一个字段
        **/
      .keyBy(0)
      .sum(1)

      /**
        * 设置并发度,相当于repartiion或者coalesce
        **/
      .setParallelism(3)
      .map(x => {
        (x._1, x._2.toString)
        //        Student(x._1, x._2)
      })

    /** 返回为tuple */


    /**
      * 设置redis的端口
      **/
    val conf = new FlinkJedisPoolConfig.Builder().setHost("flinkmaster").setPort(6379).build()
    val redisSink = new RedisSink[(String, String)](conf, new RedisExampleMapper)


    wordcount.print()
    //    wordcount.addSink(redisSink)
    val ms = new MySqlSink
    //    wordcount.addSink(ms)

    /**
      * 开始执行,相当于streaming 的action算子
      * 为显示触发
      **/
    env.execute("flink streaming")
  }
}



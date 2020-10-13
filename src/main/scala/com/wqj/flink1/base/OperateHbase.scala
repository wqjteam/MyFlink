package com.wqj.flink1.base

import java.util.Properties

import com.wqj.flink1.base.WcKafka.{broker, group_id, topic, zk}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.apache.kafka.clients.consumer.ConsumerConfig


object OperateHbase {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc1"
  private val topic = "flink_test"
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)
//    tableEnv.connect(new Kafka().version("0.11").topic("test2").property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092").property("zookeeper.connect","hadoop102:2181")
//      .property(ConsumerConfig.GROUP_ID_CONFIG,"ae"))
//      .withFormat(new Csv().fieldDelimiter(' '))
//      .withSchema(new Schema().field("name",DataTypes.STRING())
//        .field("age",DataTypes.INT()))
//      .createTemporaryTable("kafkaInputTable")

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    //kafka的consumer，test1是要消费的topic
    import org.apache.flink.api.scala._
    val kafkaSource=new FlinkKafkaConsumer( topic,new SimpleStringSchema,properties)
    val stream = env.addSource(kafkaSource).map(x => {
      x
    })
  }
}
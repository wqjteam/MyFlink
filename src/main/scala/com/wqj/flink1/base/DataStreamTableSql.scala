package com.wqj.flink1.base


import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.log4j.Logger


object DataStreamTableSql {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "DSTS2"
  private val topic = "flink_Table_test"

  def main(args: Array[String]): Unit = {
    lazy val logger = Logger.getLogger(WcKafka.getClass)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    val StreamTableEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      person(field(0).toInt, field(1), field(2).toInt)
    }).setParallelism(1)
    val table = StreamTableEnv.fromDataStream(stream)
    StreamTableEnv.createTemporaryView("person", table)
    //转为table之后还是要通过sink进行保存
    import org.apache.flink.api.scala._
    val result = StreamTableEnv.sqlQuery("select * from person where id < 100")
    StreamTableEnv.toAppendStream[person](table).print()
    StreamTableEnv.toAppendStream[person](result).print()
    env.execute("stream_table_task")
  }
}

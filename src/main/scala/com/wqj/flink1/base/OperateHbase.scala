package com.wqj.flink1.base

import java.util.Properties

import com.wqj.flink1.sink.HbaseSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
import org.apache.flink.table.descriptors._


object OperateHbase {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc1"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
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
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      person(field(0).toInt, field(1), field(2).toInt)
    })
    tableEnv.createTemporaryView("person", tableEnv.fromDataStream(stream))
    val kafkastream=tableEnv.sqlQuery("select * from person")
    val streamresult=tableEnv.toAppendStream[person](kafkastream)
    streamresult.addSink(new HbaseSink)
    streamresult.print()
//    tableEnv.connect(
//      new HBase()
//        .version("1.4.3") // required: currently only support "1.4.3"
//        .tableName("person") // required: HBase table name
//        .zookeeperQuorum("192.168.4.110:2181") // required: HBase Zookeeper quorum configuration
//        .zookeeperNodeParent("/hbase") // optional: the root dir in Zookeeper for HBase cluster.The default value is "/hbase".
//        .writeBufferFlushMaxSize("10mb") // optional: writing option, determines how many size in memory of buffered
//        // rows to insert per round trip. This can help performance on writing to JDBC
//        // database. The default value is "2mb".
//        .writeBufferFlushMaxRows(1000) // optional: writing option, determines how many rows to insert per round trip.
//        // This can help performance on writing to JDBC database. No default value,
//        // i.e. the default flushing is not depends on the number of buffered rows.
//        .writeBufferFlushInterval("2s") // optional: writing option, sets a flush interval flushing buffered requesting
//      // if the interval passes, in milliseconds. Default value is "0s", which means
//      // no asynchronous flush thread will be scheduled.
//    ).withSchema(new Schema().field("id",DataTypes.INT()).field("info",DataTypes.ROW(
//      DataTypes.FIELD("name",DataTypes.STRING()),
//      DataTypes.FIELD("age",DataTypes.INT())
//    ))).withFormat(new Csv).createTemporaryTable("hbasePerson")
//    val re=tableEnv.sqlQuery("select id as rowkey,ROW(name,age) as info from person")
//    tableEnv.insertInto("hbasePerson",re)



    env.execute("OperateHbase")
  }
}

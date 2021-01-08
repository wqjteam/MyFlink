package com.wqj.flink1.base

import java.util.Properties

import com.google.gson.Gson
import com.wqj.flink1.connectDim._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings





object OpeaterDim {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc2"
  private val topic = "flink_dim"
  val properties = new Properties()
  properties.setProperty("zookeeper.connect", zk)
  properties.setProperty("bootstrap.servers", broker)
  properties.setProperty("group.id", group_id)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    //    val tableEnv = StreamTableEnvironment.create(env, settings)
    //    val hive = new HiveCatalog("myhive", "study", FileUtil.getHadoopConf(), "2.3.4")
    //    tableEnv.registerCatalog("myhive", hive)
    //    tableEnv.useCatalog("myhive")
    //    tableEnv.useDatabase("study")
    //    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
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
    }).map(new JDBCSyncMapFunction).map(new Gson().toJson(_))
    AsyncDataStream.unorderedWait(stream,new HbaseAsyncLRU_scala,1000,java.util.concurrent.TimeUnit.MICROSECONDS).print()

    env.execute("OpeaterDim")
  }
}

package com.wqj.flink.base

import java.util.Properties

import com.wqj.flink.utils.FileUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.util.Collector

object SideOutputStream {
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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val hive = new HiveCatalog("myhive", "study", FileUtil.getHadoopConf(), "2.3.4")
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
    val KafkaStream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      Person(field(0).toInt, field(1), field(2).toInt)
    })
    //定义侧输出流
    val outputTag = OutputTag[String]("side-output")
    val mainStream = KafkaStream.process(new ProcessFunction[Person, Person] {
      override def processElement(value: Person, ctx: ProcessFunction[Person, Person]#Context, out: Collector[Person]): Unit = {
        out.collect(value)

        //发送侧侧输出流
        ctx.output(outputTag, value.toString)
      }
    })

    //获得侧输出流,如果直接加两个sink,则数据会减少
    val sideOutputStream: DataStream[String] = mainStream.getSideOutput(outputTag)
  }
}

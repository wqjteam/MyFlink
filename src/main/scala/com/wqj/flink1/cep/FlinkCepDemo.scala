package com.wqj.flink1.cep

import java.util
import java.util.Properties

import com.wqj.flink1.base.DataStreamTableSql.{broker, group_id, topic, zk}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.util.Collector
//cep的使用demo

// 定义样例类，Event用来存放输入数据，Warning存放输出数据
case class Event(deviceId: String, tempBefore: String, tempNow: String, timestamp: String)
case class Warning(deviceId: String, tempNow: String, warningMsg: String)

//设备Id,过去温度,当前温度,时间戳
//dev_1,31,32,1558430841
//dev_1,32,33,1558430842
//dev_1,33,34,1558430843
//dev_1,34,35,1558430844
//dev_1,35,36,1558430845
//dev_1,36,37,1558430846
//dev_1,37,38,1558430847
//dev_1,38,39,1558430848
//dev_1,30,40,1558430849
//dev_1,40,41,1558430850

object FlinkCepDemo {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "DSTS2"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val properties = new Properties()
    properties.setProperty("zookeeper.connect", zk)
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
//    val StreamTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val source = env.addSource(kafkaSource).map(x => {
      Event("", "", "", "")
    })

    // 定义Pattern规则，匹配当前温度大于上次的温度超过6次的数据
    val eventPattern = Pattern.begin[Event]("start")
      .where(data => data.tempNow > data.tempBefore).times(6)

    val patternStream = CEP.pattern(source, eventPattern)


    // 使用select方法来捕捉结果，EventMatch为捕捉结果的函数
    val resultDataStream = patternStream.select(new EventMatch)

    // 导出数据为文本文件，但使用StreamingFileSink方法，结果并不会输出为一个sink.txt文件，而是导出为分布式文件结构，会自动生成目录和文件
    val sink = StreamingFileSink
      .forRowFormat(new Path("src/main/resources/sink.txt"), new SimpleStringEncoder[Warning]("UTF-8"))
      .build()
    resultDataStream.addSink(sink)

    // 或者直接打印出信息
    resultDataStream.print()

    env.execute("temperature detect job")
  }
}

// 重写PatternSelectFunction方法，用Warning样例类来接收数据
class EventMatch() extends PatternSelectFunction[Event, Warning] {
  override def select(pattern: util.Map[String, util.List[Event]]): Warning = {
    val status = pattern.get("start").get(0)
    Warning(status.deviceId, status.tempNow, "Temperature High Warning")
  }
}

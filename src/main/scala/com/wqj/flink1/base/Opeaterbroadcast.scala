package com.wqj.flink1.base

import java.util.Properties

import com.google.gson.Gson
import com.wqj.flink1.intput.HbaseReader
import com.wqj.flink1.output.HbaseBroadCastProcessFunction
import com.wqj.flink1.utils.FileUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.catalog.hive.HiveCatalog

object Opeaterbroadcast {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc2"
  private val topic = "flink_test"
  val properties = new Properties()
  properties.setProperty("zookeeper.connect", zk)
  properties.setProperty("bootstrap.servers", broker)
  properties.setProperty("group.id", group_id)

  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    //隐式转化

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
    val hiveDataSet = tableEnv.sqlQuery("select * from study.person")
    /**
      * kafka的consumer，flink_test是要消费的topic
      **/
    val kafkaSource = new FlinkKafkaConsumer(topic, new SimpleStringSchema, properties)
    val stream = env.addSource(kafkaSource).map(x => {
      val field = x.split(",")
      Person(field(0).toInt, field(1), field(2).toInt)
    })
    tableEnv.createTemporaryView("StreamPerson", stream)

    /**
      * hbase数据导入
      **/
    val hbasesource = env.addSource(new HbaseReader).map(tp => {
      //      Person(field(0).toInt, field(1), field(2).toInt)
      val person = new Gson().fromJson(tp._2, classOf[Person])
      //      (tp._1, person)
      person
    })
    //    tableEnv.createTemporaryView("hbasePerson", hbasesource)
    //    val hbseAndkafkaResult = tableEnv.sqlQuery("select * from hbasePerson union select * from StreamPerson")
    //    val adkkr = tableEnv.toRetractStream[Person](hbseAndkafkaResult)
    //    adkkr.print()


    //广播状态描述符,广播流只支持MapState的结构
    val broadcasteStateDescriptor = new MapStateDescriptor[String, Person]("broadcasteStateDescriptor", classOf[String], classOf[Person])

    //使用广播状态的描述符进行广播
    val hbasebroadcast = hbasesource.setParallelism(1).broadcast(broadcasteStateDescriptor)


    //把hbase的历史数据和新来的数据做合并
    val MergeStream = stream.connect(hbasebroadcast).process(new HbaseBroadCastProcessFunction).map(x=>x.toString).addSink(new FlinkKafkaProducer[String]("localhost:9092","flinktest3",new SimpleStringSchema()))
    env.execute("Opeaterbroadcast")
  }
}

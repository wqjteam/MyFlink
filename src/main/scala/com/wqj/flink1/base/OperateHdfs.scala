package com.wqj.flink1.base

import java.time.ZoneId
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.wqj.flink1.output.HDFSSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.log4j.Logger
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object OperateHdfs {
  private val zk = "flinkmaster:2181"
  private val broker = "flinkmaster:9092"
  private val group_id = "wc1"
  private val topic = "flink_test"

  def main(args: Array[String]): Unit = {
    lazy val logger = Logger.getLogger(WcKafka.getClass)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(10)
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
    }).setParallelism(1)
    stream.addSink(StreamingFileSink
      .forRowFormat(new Path("hdfs://192.168.4.110:9000/tmpfile/txt/"), new SimpleStringEncoder[Person]("utf-8"))
      .withRollingPolicy(
      DefaultRollingPolicy.builder()
        .withRolloverInterval(TimeUnit.SECONDS.toSeconds(10)) //10min 生成一个文件
        .withInactivityInterval(TimeUnit.SECONDS.toSeconds(5)) //5min未接收到数据，生成一个文件
        .withMaxPartSize( 10) //文件大小达到1G
        .build())
      .build())
    stream.print()
    env.execute("OperateHdfs")
  }
}

package com.wqj.flink1.base

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import com.wqj.flink1.pojo.{RedisBasePojo, Student}
import com.wqj.flink1.output.{MySqlSink, RedisExampleMapper}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
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
  private val group_id = "w2"
  private val topic = "flink_test_student"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    lazy val logger = Logger.getLogger(WcKafka.getClass)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(50)
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
      .flatMap(_.split(","))
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
        Student(x._1, x._2)
        //        Student(x._1, x._2)
      })

    /** 返回为tuple */


    /**
      * 设置redis的端口
      **/
    val conf = new FlinkJedisPoolConfig.Builder().setHost("flinkmaster").setPort(6379).build()
    val redisSink = new RedisSink[RedisBasePojo](conf, new RedisExampleMapper)


//    wordcount.print()
    //    wordcount.addSink(redisSink)
    val ms = new MySqlSink
    wordcount.addSink(ms)
    wordcount.addSink(StreamingFileSink
      .forRowFormat(new Path("hdfs://192.168.4.110:9000/tmpfile/studenttxt2/"), new SimpleStringEncoder[Student]("utf-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.SECONDS.toSeconds(5)) //10min 生成一个文件
          .withInactivityInterval(TimeUnit.SECONDS.toSeconds(2)) //5min未接收到数据，生成一个文件
          .withMaxPartSize( 10) //文件大小达到1G
          .build())
      .build())
    wordcount.print()
    /**
      * 开始执行,相当于streaming 的action算子
      * 为显示触发
      **/
    env.execute("flink streaming")
  }
}



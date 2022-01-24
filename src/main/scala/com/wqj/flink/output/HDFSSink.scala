package com.wqj.flink.output

import java.time.ZoneId
import java.util.concurrent.TimeUnit

import com.wqj.flink.base.Person

import scala.reflect._
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{OutputFileConfig, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.{DefaultRollingPolicy, OnCheckpointRollingPolicy}

class HDFSSink[T](path: String) {

  private val filePath = "hdfs://192.168.4.110:9000" + path
  //定义桶
  private val assigner = new DateTimeBucketAssigner[Person]("yyyy-MM-dd--HH", ZoneId.of("Asia/Shanghai"))
  //文件名
  private val config = OutputFileConfig.builder()
    .withPartPrefix("prefix")
    .withPartSuffix(".txt")
    .build()


  // text  sink
  val sinkRow = StreamingFileSink
    .forRowFormat(new Path(filePath + "/txt/"), new SimpleStringEncoder[T]("utf-8"))
    //      .withBucketAssigner(new MyBucketAssigner())
    .withRollingPolicy(
    DefaultRollingPolicy.builder()
      .withRolloverInterval(TimeUnit.SECONDS.toSeconds(10)) //10min 生成一个文件
      .withInactivityInterval(TimeUnit.SECONDS.toSeconds(5)) //5min未接收到数据，生成一个文件
      //      .withRolloverInterval(TimeUnit.MINUTES.toMillis(10)) //10min 生成一个文件
      //      .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) //5min未接收到数据，生成一个文件
      .withMaxPartSize(1024 * 1024 * 1024) //文件大小达到1G
      .build())
    //    .withOutputFileConfig()
    .build()
  val sinkParquet = StreamingFileSink.forBulkFormat(new Path(filePath + "/parquet/"),
    ParquetAvroWriters.forReflectRecord(classOf[Person])).withBucketAssigner(assigner).build()


  // parquet + 压缩
  //  val sinkCloCom = StreamingFileSink.forBulkFormat(new Path(""),
  //    PualParquetAvroWriters.forReflectRecord(classOf[person], CompressionCodecName.SNAPPY)
  //  )
  //    .withBucketAssigner(assigner)
  //    .build()
}

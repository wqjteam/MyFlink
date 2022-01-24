package com.wqj.flink.utils

import java.sql.Timestamp

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HTableDescriptor, TableName}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object HbaseOperatTable {


  /**
    *
    * spark获取hbase数据
    * */
  def getTable(spark: SparkSession, tableName: String, colMap: mutable.TreeMap[String, Seq[StructField]], schema: StructType, regex: String): DataFrame = {

    // 如果表不存在，则创建表
    val admin = HbaseUtils.getHbaseAdmin()
    val conf = HbaseUtils.getHbaseInstance().configuration
    conf.set(HConstants.BASE_NAMESPACE_DIR, tableName.split(":")(0))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }
    //过滤器的添加
    val filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL)

    val rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));


    filterlist.addFilter(rf)
    val scan = new Scan().setFilter(filterlist)

    conf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))

    //读取数据并转化成rdd TableInputFormat 是 org.apache.hadoop.hbase.mapreduce 包下的
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val hBaseRowRDD = hBaseRDD.map { case (_, result) =>
      //获取行键
      val key = Bytes.toString(result.getRow)
      //通过列族和列名获取列
      var seq: Seq[Any] = Seq()
      colMap.iterator.foreach(kv => {
        val colfamily = kv._1
        val colarr = kv._2.toArray

        colarr.foreach(col => {
          if ("ods_insert_time".equals(col.name) || "ods_update_time".equals(col.name)) {
            seq = seq :+ Timestamp.valueOf(Bytes.toString(result.getValue(colfamily.getBytes, col.name.getBytes)))

          } else {

            seq = seq :+ (Bytes.toString(result.getValue(colfamily.getBytes, col.name.getBytes)))
          }
        })

      })
      Row.fromSeq(seq)
    }


    //    hBaseRowRDD.take(1).foreach(x => {
    //      println(x.size)
    //    })
    return spark.createDataFrame(hBaseRowRDD, schema)
  }

  //
  //  def deleteTableData(spark: SparkSession, tableName: String, map: mutable.TreeMap[String, Array[String]], schema: StructType): Unit = {
  //
  //    val conf = HBaseConfiguration.create()
  //    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  //    conf.set("hbase.zookeeper.quorum", ConfUtils.HDAOOPIP)
  //    //设置zookeeper连接端口，默认2181
  //    conf.set("hbase.zookeeper.property.clientPort", ConfUtils.ZKPORT)
  //    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  //    val admin = HbaseUtils.getHbaseAdmin()
  //    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
  //      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
  //      admin.createTable(tableDesc)
  //    }
  //    val delete: Delete = new Delete(Bytes.toBytes(""));
  //    val table = new HTable(conf, tableName)
  //    table.delete(delete)
  //    table.close()
  //
  //  }
}



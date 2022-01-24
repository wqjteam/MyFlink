package com.wqj.flink.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object SparkDeleteHivepartition {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      sparkBuilder.master("local[*]")
    }
    val spark: SparkSession = sparkBuilder.appName("SparkDeleteHivepartition")
      .enableHiveSupport()
      .getOrCreate()
    val tableArray = Array(
      //ods层
      ("ods", "customer")
      , ("ods", "lineitem")
      //      , ("ods", "lineitem_hbase")
      , ("ods", "nation")
      , ("ods", "orders")
      //      , ("ods", "orders_hbase")
      , ("ods", "part")
      , ("ods", "partsupp")
      , ("ods", "region")
      , ("ods", "supplier")
      , ("ods", "part")

      //dwd层
      , ("dwd", "dim_customer")
      , ("dwd", "dim_item")
      , ("dwd", "dim_nation")
      , ("dwd", "dim_part")
      , ("dwd", "dim_region")
      , ("dwd", "dim_supplier")
      , ("dwd", "fact_orders")
      , ("dwd", "fact_orders_lineitem")
      , ("dwd", "fact_partsupp")

      //dws层
      , ("dws", "customer_day_consumption_aggr")
      , ("dws", "customer_month_consumption_aggr")

    )

    tableArray.foreach(table => {

      deletePartition(spark, table._2, table._1)

    })

  }

  //保留近三个的分区
  def deletePartition(spark: SparkSession, table: String, database: String = "ods", partitionname: String = "etldate"): Unit = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val calendar: Calendar = Calendar.getInstance
    calendar.add(Calendar.DATE, -(3))
    val date = calendar.getTime
    val datestr = sdf.format(date)

    //删除分区
    val partitons = spark.sql(s"show partitions ${database}.${table}").collect.filter(x => {
      null != x && x.length != 0
    }).map(x => {
      x.toString().replace("]", "")
        .replace("[", "").split("=")(1)
    }).sortBy[Int](x => {
      x.toInt
    })

    if (partitons.length > 3) {
      for (index <- 0 until partitons.length - 3) {
        spark.sql(s"ALTER TABLE ${database}.${table} DROP PARTITION (${partitionname}='${partitons(index)}')")
      }
    }
  }
}

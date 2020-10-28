package com.wqj.flink1.output

import java.util
import java.util.List

import com.google.gson.Gson
import com.wqj.flink1.base.Person
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._

import scala.collection.mutable

class FileProcessWindowFunction extends ProcessAllWindowFunction[String, Array[String], TimeWindow] {


  override def process(context: Context, elements: Iterable[String], out: Collector[Array[String]]): Unit = {
    elements.foreach(x => {
      println("接收到输入:" + x)
    })
    out.collect(elements.toArray)

    //    table.put(puts)
    //    table.close()
    //    conn.close()
  }

}


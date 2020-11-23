package com.wqj.flink1.output

import com.wqj.flink1.base.Person
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

class HbaseProcessFunction extends BroadcastProcessFunction[Person, Person, Person] {
  var processMark = 0
  //广播状态描述符,广播流只支持MapState的结构,支持吃kv
  val broadcasteStateDescriptor = new MapStateDescriptor[String, Person]("broadcasteStateDescriptor", classOf[String], classOf[Person])

  //先通过processBroadcastElement(初始化,才会进来),再通过processElement
  override def processElement(value: Person, ctx: BroadcastProcessFunction[Person, Person, Person]#ReadOnlyContext, out: Collector[Person]): Unit = {
    val hbasebc: ReadOnlyBroadcastState[String, Person] = ctx.getBroadcastState(broadcasteStateDescriptor)
    val iter = hbasebc.immutableEntries().iterator()
    val perarr: ArrayBuffer[Person] = new ArrayBuffer[Person]()
    while (iter.hasNext) {
      perarr += iter.next().getValue
    }
//    if (processMark == 0) {
//      processMark = 1
//    }
    out.collect(value)
  }

  override def processBroadcastElement(value: Person, ctx: BroadcastProcessFunction[Person, Person, Person]#Context, out: Collector[Person]): Unit = {
    //第一次初始化的时候才会进这个方法
    val hbasebc: BroadcastState[String, Person] = ctx.getBroadcastState(broadcasteStateDescriptor)
    val iter = hbasebc.entries().iterator()

    hbasebc.put(value.id.toString, value)
  }
}

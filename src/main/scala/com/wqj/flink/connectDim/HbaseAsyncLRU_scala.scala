package com.wqj.flink.connectDim

import java.util
import java.util.{ArrayList, Collections}
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.gson.{JsonObject, JsonParser}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.hbase.async.{GetRequest, HBaseClient, KeyValue}

import scala.collection.JavaConversions._

class HbaseAsyncLRU_scala extends RichAsyncFunction[String, String] {
  val table: String = "study:person"
  var cache: Cache[String, String] = null
  var client: HBaseClient = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //创建hbase客户端
    client = new HBaseClient("192.168.4.110", "2181")
    cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(60, TimeUnit.SECONDS).build[String, String]
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    val jo: JsonObject = new JsonParser().parse(input).getAsJsonObject
    val id = jo.get("int").getAsInt
    val cacheName = cache.getIfPresent(id)
    if (cacheName != null) {
      jo.addProperty("name", cacheName)
      resultFuture.complete(Collections.singleton(jo.toString))
    } else {
      client.get(new GetRequest(table, String.valueOf(id))).addCallback(new com.stumbleupon.async.Callback[String, util.ArrayList[KeyValue]] {
        override def call(t: util.ArrayList[KeyValue]): String = {
          t.foreach(kv => {
            val value = new String(kv.value)
            jo.addProperty("name", value)
            resultFuture.complete(Collections.singleton(jo.toString))
            cache.put(String.valueOf(id), value)
          })


          return null
        }
      }
      )
    }
  }

  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    super.timeout(input, resultFuture)
  }

  override def close(): Unit = {
    super.close()
  }
}

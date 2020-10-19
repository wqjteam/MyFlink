package com.wqj.flink1.output

import java.util
import org.elasticsearch.client.Requests
import com.wqj.flink1.base.Person
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.http.HttpHost
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

class ElasticSearchSink extends ElasticsearchSinkFunction[Person] {
  override def process(t: Person, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    print("saving data" + t)
    //包装成一个Map或者JsonObject
    val hashMap = new util.HashMap[String, String]()
    hashMap.put("name", t.name.toString)
    hashMap.put("age", t.age.toString)
    //创建index request,准备发送数据
    val indexRequest = Requests.indexRequest().index("sensor").id(t.id.toString).`type`("doc").source(hashMap)
    //发送请求,写入数据
    requestIndexer.add(indexRequest)
    println("data saved successfully")

  }
}

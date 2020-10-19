package com.wqj.flink1.output

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import java.net.InetAddress

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings

import scala.collection.JavaConversions
class ElasticSearchOutput extends OutputFormat[String] {
  var client: TransportClient = _
  override def configure(parameters: Configuration): Unit = {
//    val settings = Settings.settingsBuilder().put("cluster.name", "iteblog").build()
//    val nodes = "www.iteblog.com"
//    val transportAddress = nodes.split(",").map(node =>
//      new InetSocketTransportAddress(InetAddress.getByName(node), 9003))
//    client = TransportClient.builder().settings(settings).build()
//      .addTransportAddresses(transportAddress: _*)
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = ???

  override def writeRecord(record: String): Unit = ???

  override def close(): Unit = ???
}

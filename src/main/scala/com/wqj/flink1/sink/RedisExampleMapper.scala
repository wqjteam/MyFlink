package com.wqj.flink1.sink

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 在flink on yarn集群中直接写入redis
  **/
class RedisExampleMapper extends RedisMapper[(String, String)] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET, null)
  }

  /**
    * 该方法可以直接通过.map(_=>{_.1})或者.map(_=>{_.2})提取
    **/
  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2


}

package com.wqj.flink1.sink

import com.wqj.flink1.pojo.RedisBasePojo
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 在flink on yarn集群中直接写入redis
  **/
class RedisExampleMapper extends RedisMapper[(RedisBasePojo)] {
  override def getCommandDescription: RedisCommandDescription = {
//    new RedisCommandDescription(RedisCommand.SET, null)
    new RedisCommandDescription(RedisCommand.HSET, "person_base")
  }

  /**
    * 该方法可以直接通过.map(_=>{_.1})或者.map(_=>{_.2})提取
    **/
  override def getKeyFromData(data: RedisBasePojo): String = data.id

  override def getValueFromData(data: RedisBasePojo): String = data.value


}

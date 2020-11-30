package com.wqj.flink1.connectDim;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stumbleupon.async.Callback;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class HbaseAsyncLRU extends RichAsyncFunction<String, String> {

    String table = "person";
    Cache<String, String> cache = null;
    private HBaseClient client = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //创建hbase客户端
        client = new HBaseClient("127.0.0.1", "7071");
        cache = CacheBuilder.newBuilder()
                //最多存储10000条
                .maximumSize(10000)
                //过期时间为1分钟
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        JsonObject jo = new JsonParser().parse(input).getAsJsonObject();
        int id = jo.get("int").getAsInt();
//        int age = jo.get("age").getAsInt();
        //读缓存
        String cacheName = cache.getIfPresent(id);
        if (cacheName != null) {
            jo.addProperty("name", cacheName);
            resultFuture.complete(Collections.singleton(jo.toString()));
        } else {
            //如果缓存获取失败再从hbase获取维度数据
            client.get(new GetRequest(table, String.valueOf(id)))
                    .addCallback((Callback<String, ArrayList<KeyValue>>)
                            arg -> {
                                for (KeyValue kv : arg) {
                                    //kv.value()应该不止一列数据,有很多的数据
                                    String value = new String(kv.value());
                                    jo.addProperty("name", value);
                                    resultFuture.complete(Collections.singleton(jo.toString()));
                                    cache.put(String.valueOf(id), value);
                                }
                                return null;
                            });
        }
    }


    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (client != null) {
//            client.shutdown();
        }
    }
}

package com.wqj.flink1.ConnectDim;

import com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.TimeUnit;

public class HbaseAsyncLRU extends RichAsyncFunction<String, String> {

    private HBaseClient hBaseClient;

    private LoadingCache<Long, Optional<Item>> _memberCache;

    private String tableName;

    public HbaseAsyncLRU(String tableName,String zk){
        this.hBaseClient=new HBaseClient(zk);
        this.tableName=tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        _memberCache= CacheBuilder
                .newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .build(new CacheLoader<Long, Optional<Item>>(){

                    @Override
                    public Optional<Item> load(Long memberId) throws Exception {

                        //执行hbase查询工作
                        GetRequest get=new GetRequest(tableName,memberId.toString());
                        hBaseClient.get(get).addCallbacks(new Callback<String, ArrayList<KeyValue>>() {
                            @Override
                            public String call(ArrayList<KeyValue> keyValues) throws Exception {
                                //todo
                                return null;
                            }
                        }, new Callback<String, Exception>() {
                            @Override
                            public String call(Exception e) throws Exception {
                                //todo
                                return null;
                            }
                        });

                        return Optional.empty();
                    }
                });
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {

    }
    @Override
    public void close() throws Exception {
        super.close();
    }
}

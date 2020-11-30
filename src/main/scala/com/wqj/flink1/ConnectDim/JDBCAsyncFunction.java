package com.wqj.flink1.ConnectDim;


import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.List;

public class JDBCAsyncFunction extends RichAsyncFunction<String, String> {
    private SQLClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        Vertx vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(10)
                .setEventLoopPoolSize(10));

        JsonObject config = new JsonObject()
                .put("url", "jdbc:mysql://192.168.4.110:3306/flink_test")
                .put("driver_class", "com.mysql.cj.jdbc.Driver")
                .put("max_pool_size", 10)
                .put("user", "x")
                .put("password", "x");

        client = JDBCClient.createShared(vertx, config);
    }


    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        client.getConnection(conn -> {
            if (conn.failed()) {
                return;
            }

            final SQLConnection connection = conn.result();
            connection.query("select id, name from person where id = " + input, res2 -> {
                ResultSet rs = new ResultSet();
                if (res2.succeeded()) {
                    rs = res2.result();
                }

                List<String> stores = new ArrayList<String>();
                for (JsonObject json : rs.getRows()) {
//                    Store s = new Store();
//                    s.setId(json.getInteger("id"));
//                    s.setName(json.getString("name"));
                    stores.add("");
                }
                connection.close();
                resultFuture.complete(stores);
            });
        });
    }

    @Override
    public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
        List<String> list = new ArrayList();
        list.add(input);
        resultFuture.complete(list);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}

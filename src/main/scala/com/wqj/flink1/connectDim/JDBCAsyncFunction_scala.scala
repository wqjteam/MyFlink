package com.wqj.flink1.connectDim

import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import io.vertx.core.{AsyncResult, Handler, Vertx, VertxOptions}
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLClient
import io.vertx.ext.sql.SQLConnection
import org.apache.flink.configuration.Configuration
import java.util

import java.util.{ArrayList, List}

/**
//不能使用,编译无法通过
class JDBCAsyncFunction_scala extends RichAsyncFunction[String, String] {
  private var client: SQLClient = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(10).setEventLoopPoolSize(10))

    val config = new JsonObject().put("url", "jdbc:mysql://192.168.4.110:3306/flink_test").put("driver_class", "com.mysql.cj.jdbc.Driver").put("max_pool_size", 10).put("user", "x").put("password", "x")

    client = JDBCClient.createShared(vertx, config)
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    client.getConnection(new Handler[AsyncResult[SQLConnection]] {
      override def handle(event: AsyncResult[SQLConnection]): Unit = {
        //判断是否失败
        if (event.failed) return

        val connection = event.result

        connection.query("select id, name from person where id = " + input, new Handler[AsyncResult[ResultSet]] {
          override def handle(event2: AsyncResult[ResultSet]): Unit = {
            var rs = new ResultSet
            if (event2.succeeded) rs = event2.result

            val stores = new util.ArrayList[String]
            import scala.collection.JavaConversions._
            for (json <- rs.getRows) { //
              stores.add("qwewqe")
            }
            connection.close()
            resultFuture.complete(stores)
          }
        })

      }
    })
  }

  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    import scala.collection.JavaConversions._
    super.timeout(input, resultFuture)
    val list = new util.ArrayList[String]
    list.add(input)
    resultFuture.complete(list)
  }

  override def close(): Unit = {
    super.close()
    client.close()
  }
}
*/
package com.wqj.flink1.base



import java.sql.DriverManager

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object OffLineWC {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("workspace/input/wordcountASSA")
    val data2 = data.map(record => {
      val basedata = record.split(";")
      val data2 = for (i <- 0 until basedata.length - 1)
        yield basedata(i) + "_" + basedata(i + 1)
      data2.toArray
    })
    val data3 = data2.flatMap(_.seq)
    val data4 = data3.map((_, 1))
    val data5 = data4.groupBy(0)
    val data6 = data5.sum(1)
    data6.print()


    val elements: DataSet[Array[Tuple2[String, Int]]] = env.fromElements(Array(("java", 1), ("scala", 1), ("java", 1)))

    val tuple_map = elements.flatMap(x => x)
    val group_map: GroupedDataSet[(String, Int)] = tuple_map.groupBy(x => x._1)
    //拆开里面的list，编程tuple
    //按照单词聚合
    val reduce = group_map.reduce((x, y) => (x._1, x._2 + y._2))
    reduce.print()
    val lastsink=reduce.mapPartition(record => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      val conn = DriverManager.getConnection("jdbc:mysql://192.168.4.110:3306/flink_test", "root", "123456")
      record.map(x => {
        val statement = conn.prepareStatement("insert into data_test(data,number) values(?,?)")

        statement.setString(1, x._1)
        statement.setInt(2, x._2)
        statement.execute()
      })

    })
    lastsink
//      .print()
  }
}

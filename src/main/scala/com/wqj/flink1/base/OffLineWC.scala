package com.wqj.flink1.base

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object OffLineWC {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile("workspace/input/wordcountASSA")
    val data2 = data.map(record => {
      val basedata = record.split(";")
      val data2=for (i <- 0 until basedata.length - 1)
        yield basedata(i) + "_" + basedata(i + 1)
      data2.toArray
    })
    val data3 = data2.flatMap(_.seq)
    val data4 = data3.map((_, 1))
    val data5 = data4.groupBy(0)
    val data6 = data5.sum(1)
    data6.print()


  }
}

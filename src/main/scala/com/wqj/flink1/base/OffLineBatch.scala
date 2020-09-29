package com.wqj.flink1.base

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * @Auther: wqj
  * @Date: 2019/2/18 16:05
  * @Description:
  */
object OffLineBatch {
  def main(args: Array[String]): Unit = {


    /**
      * 创建环境变量
      * */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("workspace/input/wordcount")
    /**
      * 对内存中的数据进行计算
      * */
    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { record=>(record, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}

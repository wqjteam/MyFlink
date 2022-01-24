package com.wqj.flink.utils.udf

import org.apache.flink.table.functions.AggregateFunction

import scala.collection.mutable.ListBuffer

class FlinkMedianUdaf extends AggregateFunction[Double, ListBuffer[Double]] {
  override def getValue(acc: ListBuffer[Double]): Double = {
    val length = acc.size
    val med = (length / 2)
    val seq = acc.sorted

    try {
      length % 2 match {
        case 0 => (seq(med) + seq(med - 1)) / 2
        case 1 => seq(med)
      }
    } catch {
      case e: Exception => seq.head
    }


  }

  override def createAccumulator(): ListBuffer[Double] = new ListBuffer[Double]()

  def accumulate(accumulator: ListBuffer[Double], i: Double) = {
    accumulator.append(i)
  }

  /*
 * 使用merge方法把多个accumulator合为1个accumulator
 * merge方法的第1个参数，必须是使用AggregateFunction的ACC类型的accumulator，而且第1个accumulator是merge方法完成之后，状态所存放的地方。
 * merge方法的第2个参数是1个ACC type的accumulator遍历迭代器，里面有可能存在1个或者多个accumulator。
  */
  def merge(accumulator: ListBuffer[Double], its: Iterable[ListBuffer[Double]]) = {
    its.foreach(i => accumulator ++ i)
  }

}

package com.wqj.flink.utils

import java.text.SimpleDateFormat
import java.util.Date

object TimeTransform {

  def tranTimeToString(tm: String): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm.toLong))
    tim
  }

  def tranTimeToString(tm: Long): String = {
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm))
    tim
  }

     //时间转成时间戳
     def tranTimeToLong(tm: String): Long = {
       val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
       val dt = fm.parse(tm)
       val aa = fm.format(dt)
       val tim: Long = dt.getTime()
       tim
     }

}

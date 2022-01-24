package com.wqj.flink.bean

case class WindowAggrKafka(
                            cust_key: String,
                            total_consumption: Double,
                            total_order: Int,
                            year: String,
                            month: String,
                            day: String,
                            hour: String,
                            minute: String,
                            dws_insert_time: String,
                            dws_update_time: String
                          )

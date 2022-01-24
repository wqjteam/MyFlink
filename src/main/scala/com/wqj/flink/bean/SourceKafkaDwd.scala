package com.wqj.flink.bean

case class SourceKafkaDwd(
                           order_key: String,
                           cust_key: String,
                           cust_name:String,
                           order_status: String,
                           total_price: Double,
                           order_date: String,
                           order_priority: String,
                           clerk: String,
                           ship_priority: String,
                           comment: String,
                           create_user: String,
                           create_time: String,
                           modify_user: String,
                           modify_time: Long
                         )

package com.wqj.flink.offline.dwd

import java.text.SimpleDateFormat
import java.util.Calendar

import com.wqj.flink.utils.{ConfUtils, HbaseOperatTable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable


//spark-submit --master yarn --driver-memory 4G --driver-cores 4  --num-executors 2 --executor-memory 3g --executor-cores 4 --class com.shtd.dw.offline.dwd.ToDwdAllJob /sh/MallDW-1.0-SNAPSHOT.jar yarn
object ToDwdAllJob {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      sparkBuilder.master("local[*]")
    }
    val spark = sparkBuilder.appName("ToDwdAllJob")
      .enableHiveSupport()
      .getOrCreate()


    /**
      * 连接mysql
      **/
    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "NATION").load().createTempView("mysql_nation")


    spark.sql("set spark.sql.storeAssignmentPolicy=LEGACY")
    val sdf = new SimpleDateFormat("yyyyMMdd")

    val calendar: Calendar = Calendar.getInstance
    calendar.add(Calendar.DATE, -(1))
    val date = calendar.getTime
    val datestr = sdf.format(date)


    /**
      * customer表
      *
      */

    var ods_customer_max_etldate: String = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from ods.customer").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_customer_max_etldate = maxt(0).toString
        }
      })

    var customer_max_etldate: String = ConfUtils.DEFAULTPARTITION
    spark.sql("select max(etldate) from dwd.dim_customer").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          customer_max_etldate = maxt(0).toString
        }
      })

    //合并并获取最新的数据
    spark.sql(
      s"""
         |select
         |cust_key,
         | name,
         | address,
         |  nation_key,
         |phone,
         |acctbal,
         |mktsegment,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time
         | ,first_value(insert_time) over(PARTITION BY cust_key  order by  modify_time desc) dwd_insert_time
         | ,last_value(update_time) over(PARTITION BY cust_key  order by  modify_time desc) dwd_update_time
         |
        | ,row_number() over(PARTITION BY cust_key  order by  modify_time desc) as num
         |from (
         |select
         |cust_key,
         |name,
         |address,
         |nation_key,
         |phone,
         |acctbal,
         |mktsegment,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time,
         |dwd_insert_time as insert_time,
         |dwd_update_time as update_time,
         |etldate
         | from dwd.dim_customer where  etldate="${customer_max_etldate}"
         |
        |union all
         |
        |select
         | custkey,
         |name,
         |address,
         |nationkey,
         |phone,
         |acctbal,
         |mktsegment,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time,
         |ods_insert_time as insert_time,
         |ods_update_time as update_time,
         |etldate
         |
        |
        | from ods.customer where  etldate='${ods_customer_max_etldate}'
         |) a
      """.stripMargin).createOrReplaceTempView("dwd_union_customer")
    spark.sql(
      s"""
         |insert overwrite  table dwd.dim_customer partition (etldate='${datestr}')
         |select
         |cust_key,name,address,nation_key,phone,acctbal,mktsegment,comment,create_user,create_time,
         |modify_user,modify_time,dwd_insert_time,dwd_update_time
         |from dwd_union_customer where  num=1
         |
      """.stripMargin)


    /**
      * nation表
      *
      */
    var ods_nation_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from ods.nation").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_nation_max_etldate = maxt(0).toString
        }
      })

    var nation_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from dwd.dim_nation").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          nation_max_etldate = maxt(0).toString
        }
      })


    spark.sql(
      s"""
         |
         |select *,
         | first_value(dwd_insert_time) over(PARTITION BY nation_key  order by  modify_time desc) insert_time
         | ,last_value(dwd_update_time) over(PARTITION BY nation_key  order by  modify_time desc) update_time
         |
         | ,row_number() over(PARTITION BY nation_key  order by  modify_time desc) as num
         | from (
         |select *
         | from dwd.dim_nation  where  etldate="${nation_max_etldate}"
         |union all
         |select *
         |from ods.nation a where  etldate='${ods_nation_max_etldate}'
         |) a
      """.stripMargin).createOrReplaceTempView("dwd_union_nation")
    spark.sql(
      s"""
         |insert overwrite  table dwd.dim_nation partition (etldate='${datestr}')
         |select nation_key,
         |name,
         |region_key,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time
         |
         |,insert_time
         |,update_time
         |
         |from dwd_union_nation a where  num=1
         |
      """.stripMargin)


    /**
      * region表
      *
      */
    var ods_region_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from ods.region").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_region_max_etldate = maxt(0).toString
        }
      })

    var region_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from dwd.dim_region").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          region_max_etldate = maxt(0).toString
        }
      })


    spark.sql(
      s"""
         |
         |select *,
         | first_value(dwd_insert_time) over(PARTITION BY region_key  order by  modify_time desc) insert_time
         | ,last_value(dwd_update_time) over(PARTITION BY region_key  order by  modify_time desc) update_time
         |
         | ,row_number() over(PARTITION BY region_key  order by  modify_time desc) as num
         | from (
         |select *
         | from dwd.dim_region  where  etldate="${region_max_etldate}"
         |union all
         |select *
         |from ods.region a where  etldate='${ods_region_max_etldate}'
         |) a
      """.stripMargin).createOrReplaceTempView("dwd_union_region")
    spark.sql(
      s"""
         |insert overwrite  table dwd.dim_region partition (etldate='${datestr}')
         |select
         |region_key,
         |name,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time
         |
         |,insert_time
         |,update_time
         |
         |from dwd_union_region a where  num=1
         |
      """.stripMargin)

    /**
      * part表
      *
      */
    var ods_part_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from ods.part").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_part_max_etldate = maxt(0).toString
        }
      })

    var part_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from dwd.dim_part").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          part_max_etldate = maxt(0).toString
        }
      })


    spark.sql(
      s"""
         |
         |select *,
         | first_value(dwd_insert_time) over(PARTITION BY part_key  order by  modify_time desc) insert_time
         | ,last_value(dwd_update_time) over(PARTITION BY part_key  order by  modify_time desc) update_time
         |
         | ,row_number() over(PARTITION BY part_key  order by  modify_time desc) as num
         | from (
         |select *
         | from dwd.dim_part  where  etldate="${nation_max_etldate}"
         |union all
         |select *
         |from ods.part a where  etldate='${ods_part_max_etldate}'
         |) a
      """.stripMargin).createOrReplaceTempView("dwd_union_part")
    spark.sql(
      s"""
         |insert overwrite  table dwd.dim_part partition (etldate='${datestr}')
         |select
         |part_key,
         |name,
         |mfgr,
         |brand,
         |type,
         |size,
         |container,
         |retail_price,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time,
         |insert_time,
         |update_time
         |
         |from dwd_union_part a where  num=1
         |
      """.stripMargin)


    /**
      * dim_supplier表
      *
      */
    var ods_supplier_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from ods.supplier").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_supplier_max_etldate = maxt(0).toString
        }
      })

    var supplier_max_etldate = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(etldate) from dwd.dim_supplier").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          supplier_max_etldate = maxt(0).toString
        }
      })


    spark.sql(
      s"""
         |
         |select *,
         | first_value(dwd_insert_time) over(PARTITION BY supp_key  order by  modify_time desc) insert_time
         | ,last_value(dwd_update_time) over(PARTITION BY supp_key  order by  modify_time desc) update_time
         |
         | ,row_number() over(PARTITION BY supp_key  order by  modify_time desc) as num
         | from (
         |select *
         | from dwd.dim_supplier  where  etldate="${nation_max_etldate}"
         |union all
         |select *
         |from ods.supplier a where  etldate='${ods_supplier_max_etldate}'
         |) a
      """.stripMargin).createOrReplaceTempView("dwd_union_supplier")
    spark.sql(
      s"""
         |insert overwrite  table dwd.dim_supplier partition (etldate='${datestr}')
         |select
         |supp_key,
         |name,
         |address,
         |nation_key,
         |phone,
         |acctbal,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time,
         |insert_time,
         |update_time
         |
         |from dwd_union_supplier a where  num=1
         |
      """.stripMargin)


    /**
      * fact_partsupp
      *
      */
    var ods_partsupp_max_etldate: String = ConfUtils.DEFAULTPARTITION
    spark.sql("select max(etldate) from ods.partsupp").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_partsupp_max_etldate = maxt(0).toString
        }
      })


    val psdata = spark.sql(
      s"""
         |
         |select a.* from ods.partsupp a where etldate="${ods_partsupp_max_etldate}"
         |
      """.stripMargin)
    val psschema: StructType = psdata.schema.add(StructField("part_supp_key", LongType))
    val pdfRDD: RDD[(Row, Long)] = psdata.rdd.zipWithIndex()

    val prowRDD: RDD[Row] = pdfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))

    spark.createDataFrame(prowRDD, psschema).createOrReplaceTempView("spark_partsupp")
    spark.sql(
      s"""
         |
        |insert overwrite  table dwd.fact_partsupp partition (etldate='${datestr}')
         |select
         |part_supp_key,
         |partkey,
         |suppkey,
         |availqty,
         |supplycost,
         |comment,
         |create_user,
         |create_time,
         |modify_user,
         |modify_time,
         |ods_insert_time,
         |ods_update_time
         |from spark_partsupp
      """.stripMargin)


    /**
      * fact_orders
      *
      */
    var ods_orders_max_etldate: String = ConfUtils.DEFAULTPARTITION
    spark.sql("select max(etldate) from ods.orders").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_orders_max_etldate = maxt(0).toString
        }
      })
    //获取orders_hbase前一天的数据 采用正则过滤rowkey 随机数（0到9）+yyyyMMdd+orderkey
    val odsorderdf = spark.sql(
      s"""
         |
         |select * from
         |ods.orders
         |where etldate="${ods_orders_max_etldate}"
         |and coalesce(custkey+0,null) is not null
      """.stripMargin)
    odsorderdf.drop("etldate").createOrReplaceTempView("spark_pre_orders")
    val ordershbasechema = odsorderdf.drop("etldate").schema
    HbaseOperatTable
      .getTable(spark, "ods:orders_hbase",
        mutable.TreeMap("info" -> ordershbasechema),
        ordershbasechema, s"[0-9]${datestr}[0-9]+")
      .createOrReplaceTempView("spark_pre_orders_hbase")


    spark.sql(
      s"""
         |insert overwrite  table dwd.fact_orders partition (etldate='${datestr}')
         |select *
         | from
         |
         |  (
         |
         |  select * from spark_pre_orders
         |  union all
         |  select * from spark_pre_orders_hbase
         |  ) a
         |
         |
         | where coalesce(custkey+0,null) is not null
         |
         |
      """.stripMargin)


    /**
      * fact_orders_lineitem
      *
      */
    var ods_lineitem_max_etldate: String = ConfUtils.DEFAULTPARTITION
    spark.sql("select max(etldate) from ods.lineitem").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          ods_lineitem_max_etldate = maxt(0).toString
        }
      })


    val lineitemodsdata = spark.sql(
      s"""
         |
         |select * from ods.lineitem a where etldate="${ods_lineitem_max_etldate}"
         |
         |""".stripMargin)

    //获取lineitem_hbase前一天的数据 采用正则过滤rowkey 随机数（0到9）+yyyyMMdd+orderkey+partkey+suppkey
    val lineitemechema = lineitemodsdata.drop("etldate").schema

    HbaseOperatTable
      .getTable(spark, "ods:lineitem_hbase",
        mutable.TreeMap("info" -> lineitemechema),
        lineitemechema, s"[0-9]${datestr}[0-9]+")
      .createOrReplaceTempView("spark_pre_lineitem_hbase")

    lineitemodsdata.drop("etldate")
      .createOrReplaceTempView("spark_pre_lineitem")

    val lineitemdata = spark.sql(
      s"""
         |
         |select * from spark_pre_lineitem
         |union all
         |select * from spark_pre_lineitem_hbase
         |
      """.stripMargin)


    val pslineitem: StructType = lineitemdata.schema.add(StructField("lineitem_key", LongType))
    val test=lineitemdata.take(10)
    val ldfRDD: RDD[(Row, Long)] = lineitemdata.rdd.zipWithIndex()

    val lrowRDD: RDD[Row] = ldfRDD.map(tp => Row.merge(tp._1, Row(tp._2)))

    spark.createDataFrame(lrowRDD, pslineitem).createOrReplaceTempView("spark_lineitem")


    spark.sql(
      s"""
         |
         |insert overwrite  table dwd.fact_orders_lineitem partition (etldate='${datestr}')
         |select *
         | from spark_lineitem
         |
      """.stripMargin)
  }
}

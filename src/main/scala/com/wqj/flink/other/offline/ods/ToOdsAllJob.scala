package com.wqj.flink.other.offline.ods

import java.text.SimpleDateFormat
import java.util.Calendar

import com.wqj.flink.utils.ConfUtils
import org.apache.spark.sql.SparkSession

//因为网络带宽的建议，只给一个excutor 同时一个excutor中只给一个core
//spark-submit --master yarn --driver-memory 4G --driver-cores 4  --num-executors 1 --executor-memory 8g --executor-cores 1  --class com.shtd.dw.offline.ods.ToOdsAllJob /sh/MallDW-1.0-SNAPSHOT.jar yarn
object ToOdsAllJob {
  def main(args: Array[String]): Unit = {
    val sparkBuilder = SparkSession.builder()
    if ((args.length > 0 && args(0).equals("local")) || args.length == 0) {
      sparkBuilder.master("local[*]")
    }

    val spark = sparkBuilder.appName("ToOdsAllJob")
      .config("spark.network.timeout", 800)
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


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "CUSTOMER").load().createTempView("mysql_customer")


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "PART").load().createTempView("mysql_part")


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "PARTSUPP").load().createTempView("mysql_partsupp")


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "REGION").load().createTempView("mysql_region")


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "SUPPLIER").load().createTempView("mysql_supplier")


    /**
      * 由于order表存量数据量过大，将数据分区拉取
      **/
    val predicates =
      Array(
        0 -> 300000,
        300001 -> 600000
      ).map {
        case (start, end) =>
          s"cast(ORDERKEY as SIGNED) >= $start AND cast(ORDERKEY as SIGNED) <= $end"
      } :+ "cast(ORDERKEY as SIGNED)  > 600000"

    val prop = new java.util.Properties
    prop.setProperty("user", ConfUtils.MYSQLUSER)
    prop.setProperty("password", ConfUtils.MYSQLPASSWORD)
    val sparkorder = spark.read.jdbc(ConfUtils.MYSQLDBURL, "ORDERS", predicates, prop)
    //    println(sparkorder.rdd.partitions.size)

    sparkorder.createOrReplaceTempView("mysql_orders")


    spark.read.format("jdbc")
      .option("url", ConfUtils.MYSQLDBURL)
      .option("driver", ConfUtils.MYSQLDRIVER)
      .option("user", ConfUtils.MYSQLUSER)
      .option("password", ConfUtils.MYSQLPASSWORD)
      .option("dbtable", "LINEITEM").load().createTempView("mysql_lineitem")


    spark.sql("set spark.sql.storeAssignmentPolicy=LEGACY")
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance
    val zerotime = sdf2.format(calendar.getTime)
    calendar.add(Calendar.DATE, -(1))
    val date = calendar.getTime
    val datestr = sdf.format(date)


    /**
      * customer
      **/
    var customer_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(modify_time) from ods.customer").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          customer_time = maxt(0).toString
        }
      })


    spark.sql(
      s"""
         |select a.*
         |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
         |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
         |
       |from mysql_customer a
         |where  modify_time>'${customer_time}'
         |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
         |
       """.stripMargin).createOrReplaceTempView("spark_temp_customer")

    if (spark.sql("select 1 from spark_temp_customer limit 1").rdd.count() > 0) {


      spark.sql(
        s"""
           |insert overwrite  table ods.customer partition (etldate='${datestr}')
           |select *
           |from spark_temp_customer
           |
       """.stripMargin)
    }

    /**
      * nation
      **/

    var nation_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.nation").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          nation_time = maxt(0).toString

        }

      })
    spark
      .sql(

        s"""
           |select a.*
           | ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |from mysql_nation a
           |where  modify_time>'${nation_time}'
           |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      ).createOrReplaceTempView("spark_temp_nation")


    if (spark.sql("select 1 from spark_temp_nation limit 1").rdd.count() > 0) {
      spark
        .sql(
          s"""
             |insert overwrite  table ods.nation partition (etldate='${datestr}')
             |select *
             |from spark_temp_nation
      """.stripMargin
        )

    }


    /**
      * part
      **/

    var part_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.part").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          part_time = maxt(0).toString

        }

      })
    spark
      .sql(

        s"""
           |select a.*
           | ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |from mysql_part  a
           |where  modify_time>'${part_time}'
           |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      ).createOrReplaceTempView("spark_temp_part")


    if (spark.sql("select 1 from spark_temp_part limit 1").rdd.count() > 0)
      spark
        .sql(

          s"""
             |insert overwrite  table ods.part partition (etldate='${datestr}')
             |select *
             |from spark_temp_part
      """.stripMargin
        )


    /**
      * partsupp
      **/

    var partsupp_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.partsupp").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          partsupp_time = maxt(0).toString

        }

      })
    spark
      .sql(

        s"""
           |select a.*
           | ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |from mysql_partsupp a
           |where  modify_time>'${partsupp_time}'
           |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      ).createOrReplaceTempView("spark_temp_partsupp")


    if (spark.sql("select 1 from spark_temp_partsupp limit 1").rdd.count() > 0)
      spark
        .sql(
          s"""
             |insert overwrite  table ods.partsupp partition (etldate='${datestr}')
             |select *
             |from spark_temp_partsupp
      """.stripMargin
        )


    /**
      * region
      **/


    var region_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.region").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          region_time = maxt(0).toString

        }

      })
    spark
      .sql(

        s"""
           |select a.*
           |  ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
           | from mysql_region a
           |where  modify_time>'${region_time}'
           |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      ).createOrReplaceTempView("spark_temp_region")


    if (spark.sql("select 1 from spark_temp_region limit 1").rdd.count() > 0)
      spark
        .sql(

          s"""
             |insert overwrite  table ods.region partition (etldate='${datestr}')
             |select *
             | from spark_temp_region
      """.stripMargin
        )


    /**
      * supplier
      **/

    var supplier_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.supplier").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          supplier_time = maxt(0).toString

        }

      })
    spark
      .sql(

        s"""
         |select a.*
         | ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
         |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')
         |from mysql_supplier a
         |where  modify_time>'${supplier_time}'
         |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      ).createOrReplaceTempView("spark_temp_supplier")


    if (spark.sql("select 1 from spark_temp_supplier limit 1").rdd.count() > 0)
      spark
        .sql(

          s"""
           |insert overwrite  table ods.supplier partition (etldate='${datestr}')
           |select *
           |from spark_temp_supplier
      """.stripMargin
        )


    /**
      * lineitem
      **/

    var lineitem_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark
      .sql("select max(modify_time) from ods.lineitem").rdd.take(1)
      .foreach(maxt => {

        if (maxt != null && maxt(0) != null) {
          lineitem_time = maxt(0).toString

        }

      })

    spark
      .sql(

        s"""
         |select a.*
         |  ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS') as insert_time
         |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS') as update_time
         | from mysql_lineitem a
         |where  modify_time>'${lineitem_time}'
         |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin
      )
      .createOrReplaceTempView("spark_temp_lineitem")


    if (spark.sql("select 1 from spark_temp_lineitem limit 1").rdd.count() > 0)
      spark
        .sql(

          s"""
           |insert overwrite  table ods.lineitem partition (etldate='${datestr}')
           |select *
           | from spark_temp_lineitem
       """.stripMargin
        )


    /**
      * orders
      **/
    var order_time = ConfUtils.DEFAULTTIMESTAMPLE
    spark.sql("select max(modify_time) from ods.orders").rdd.take(1)
      .foreach(maxt => {
        if (maxt != null && maxt(0) != null) {
          order_time = maxt(0).toString
        }
      })
    spark.sql(
      s"""
         |select a.*
         | ,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS')  as insert_time
         |,from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss.SSS') as update_time
         |from mysql_orders a
         |where  modify_time>'${order_time}'
         |and modify_time<'${zerotime + ConfUtils.DEFAULTZEROTIME}'
      """.stripMargin)
      .createOrReplaceTempView("spark_temp_orders")

    if (spark.sql("select 1 from spark_temp_orders limit 1").rdd.count() > 0)
      spark.sql(
        s"""
           |insert overwrite  table ods.orders partition (etldate='${datestr}')
           |select *
           |from spark_temp_orders
      """.stripMargin)

  }
}

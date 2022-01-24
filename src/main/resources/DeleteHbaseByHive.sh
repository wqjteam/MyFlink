#!/bin/bash
nowdate=`date -d -3day +%Y%m%d`
export JAVA_HOME=/opt/module/jdk1.8.0_212
echo ”hive查询hbase$nowdate 天以前的数据“
/opt/module/hive-3.1.2/bin/hive -e "select concat('deleteall \'ods:orders_hbase\',\'',row_key,'\'')  from ods.orders_hbase where substr(row_key,2,8)<='$nowdate'" > /sh/del_hbase_orders.txt
sed -i '1,2d' /sh/del_hbase_orders.txt
/opt/module/hive-3.1.2/bin/hive -e "select concat('deleteall \'ods:lineitem_hbase\',\'',row_key,'\'') from ods.lineitem_hbase where substr(row_key,2,8)<='$nowdate'" > /sh/del_hbase_lineitem.txt
sed -i '1,2d' /sh/del_hbase_lineitem.txt

echo ”hbase删除$nowdate 天以前的数据“

echo 'exit' >> /sh/del_hbase_orders.txt
echo 'exit' >> /sh/del_hbase_lineitem.txt
#
#/opt/module/hbase-2.2.3/bin/hbase shell /sh/del_lineitem_temp.txt > /sh/hbase_del.log
hbase shell /sh/del_hbase_orders.txt
hbase shell /sh/del_hbase_lineitem.txt

#!/bin/bash

# exec in the shell

#echo 'append the wx_member in store db'
#sqoop job --exec  shencut_store_wx_member_add
echo 'append the wx_order in store db'
#sqoop job --exec  shencut_store_wx_order_add
echo 'append the wx_member_recharge_record in store db'
sqoop job --exec  shencut_store_wx_member_recharge_record_add




echo "beigin to delete the data  in hive:dbsync.shencut_store_wx_order"
hadoop fs -rm -r hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_order
# tinyint to string
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_order  --map-column-hive product_type=STRING,type=STRING,from=STRING,pay_origin=STRING,status=STRING,pay_type=STRING,open_vip_type=STRING   --hive-database dbsync --hive-table  shencut_store_wx_order  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_order' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'
echo "load  wx_order data finished!! "

echo "beigin to delete the data  in hive:dbsync.shencut_store_wx_member"
hadoop fs -rm -r hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member

echo "beigin to load wx_member data from mysql to hive everyday"
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member   --map-column-hive  ecommerce_vip_type=STRING,vip_type=STRING,status=STRING,reg_origin=STRING,industry=STRING    --hive-database dbsync --hive-table  shencut_store_wx_member  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'

echo "load  wx_member data finished!!"
#!/bin/bash

### 针对mysql表中的tinyint(1)字段，sqoop导入到hive后存在数据为null，解决方案 --connect 添加：tinyInt1isBit=false， 同时，添加字段转换： --map-column-hive 字段名=类型
## wx_order

#first import data


# tinyint to string
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_order  --map-column-hive type=STRING,from=STRING,pay_origin=STRING,status=STRING,pay_type=STRING,open_vip_type=STRING   --hive-database dbsync --hive-table  shencut_store_wx_order  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_order' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'

##


## create job
sqoop job --create shencut_store_wx_order_add -- import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_order  --map-column-hive type=STRING,from=STRING,pay_origin=STRING,status=STRING,pay_type=STRING,open_vip_type=STRING   --hive-database dbsync --hive-table  shencut_store_wx_order  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_order' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n' --check-column id --incremental append  --last-value  323846


## exec in the shell
sqoop job --exec  shencut_store_wx_order_add




###

## wx_member

#first import data


# tinyint to string
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member   --map-column-hive  vip_type=STRING,status=STRING,reg_origin=STRING,industry=STRING    --hive-database dbsync --hive-table  shencut_store_wx_member  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'

##


## create job
sqoop job --create shencut_store_wx_member_add -- import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member  --map-column-hive  vip_type=STRING,status=STRING,reg_origin=STRING,industry=STRING    --hive-database dbsync --hive-table  shencut_store_wx_member  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n' --check-column id --incremental append  --last-value  1350500


## exec in the shell
sqoop job --exec  shencut_store_wx_member_add

## wx_member_recharge_record

#first import data

sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member_recharge_record       --hive-database dbsync --hive-table  shencut_store_wx_member_recharge_record  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member_recharge_record' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'

##


## create job
sqoop job --create shencut_store_wx_member_recharge_record_add -- import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member_recharge_record       --hive-database dbsync --hive-table  shencut_store_wx_member_recharge_record  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member_recharge_record' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n' --check-column id --incremental append  --last-value  200089


## exec in the shell
sqoop job --exec  shencut_store_wx_member_recharge_record_add


##以下脚本仅需执行一次
# wx_marketing_channel (此表数据较少，仅42条数据，不用做增量更新)
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_marketing_channel  --map-column-hive status=STRING,cdn_status=STRING,is_give_vip=STRING,vip_type=STRING,open_time=STRING   --hive-database dbsync --hive-table  shencut_store_wx_marketing_channel  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_marketing_channel' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'


# wx_member_interest_tags
sqoop import --connect jdbc:mysql://192.168.11.43:3306/store?tinyInt1isBit=false --username vp_store_read --password ppYDC##821348  -m 4  --hive-overwrite  --table wx_member_interest_tags     --hive-database dbsync --hive-table  shencut_store_wx_member_interest_tags  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member_interest_tags' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'
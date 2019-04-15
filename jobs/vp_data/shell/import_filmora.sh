#!/bin/bash
#sudo su hdfs
starttime=`date +'%Y-%m-%d %H:%M:%S'`

# 1  cbs_out:order_info add :commision,tax

echo "begin to truncate the dbsync.cbs_out_order_info "
hive -e "truncate table dbsync.cbs_out_order_info"
echo "begin to import order_info"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite  --table order_info  --columns 'order_id,customer_id,product_id,version_id,order_num,license,country,origin,order_date,amount,commission,tax'  --split-by order_id --hcatalog-database dbsync  --hcatalog-table cbs_out_order_info --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose
echo "import finished!!"





# 2 cbs_out:product_info
echo "begin to truncate the dbsync.cbs_out_product_info "
hive -e "truncate table dbsync.cbs_out_product_info"
echo "begin to import the product_info"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4  --table product_info  --columns 'product_id,name,brand_id,language_id,category_id,product_num' --split-by product_id --hcatalog-database dbsync  --hcatalog-table cbs_out_product_info --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"


# 未每日导入数据hive仓库；dbsync
# 2-1 cbs_out: customer_info
echo "begin to truncate the dbsync.cbs_out_customer_info "
hive -e "truncate table dbsync.cbs_out_customer_info"
echo "begin to import the customer_info"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table customer_info  --columns 'customer_id,country,email,created' --split-by customer_id --hcatalog-database dbsync  --hcatalog-table cbs_out_customer_info --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"



# 3 cbs_out:product_version
echo "begin to truncate the dbsync.cbs_out_product_version "
hive -e "truncate table dbsync.cbs_out_product_version"
echo "begin to import the product_version"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table product_version  --columns 'version_id, version_name' --split-by version_id --hcatalog-database dbsync  --hcatalog-table cbs_out_product_version --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"
# 4 cbs_out:product_type

echo "begin to truncate the dbsync.cbs_out_product_type "
hive -e "truncate table dbsync.cbs_out_product_type"
echo "begin to import the product_type"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table product_type  --columns 'category_id, category_name' --split-by category_id --hcatalog-database dbsync  --hcatalog-table cbs_out_product_type --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose
echo "import finished!!!"

# 5 cbs_out:sys_brand
echo "begin to truncate the dbsync.cbs_out_sys_brand "
hive -e "truncate table dbsync.cbs_out_sys_brand"

echo "begint to import the sys_brand"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table sys_brand  --columns 'brand_id,brand_name' --split-by brand_id --hcatalog-database dbsync  --hcatalog-table cbs_out_sys_brand --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose
echo "import finished!!!"

# 6 cbs_out:sys_language

echo "begin to truncate the dbsync.cbs_out_sys_language "
hive -e "truncate table dbsync.cbs_out_sys_language"


echo "begin to import the sys_language"

sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table sys_language  --columns 'language_id,language_name' --split-by language_id --hcatalog-database dbsync  --hcatalog-table cbs_out_sys_language --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"

# 6-1 cbs_out:sys_country


echo "begin to truncate the dbsync.cbs_out_sys_country "
hive -e "truncate table dbsync.cbs_out_sys_country"
echo "begin to import the sys_country"

sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table sys_country  --columns 'country_id,country_name,short_name' --split-by country_id --hcatalog-database dbsync  --hcatalog-table cbs_out_sys_country --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"

# 7 sync_ods_report_dm:dim_country_ods

echo "begin to truncate the dbsync.sync_ods_report_dm_dim_country_ods "
hive -e "truncate table dbsync.sync_ods_report_dm_dim_country_ods"

echo "begin to import the dim_country_ods"
sqoop import  "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" --connect jdbc:mysql://10.14.1.102:3306/sync_ods_report_dm?characterEncoding=utf-8 --username root --password testlalatest  -m 4 --hive-overwrite --table dim_country_ods  --columns 'country_en,country_cn'  --split-by country_en --hcatalog-database dbsync  --hcatalog-table sync_ods_report_dm_dim_country_ods --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose
echo "import finished!!!"

# 8 f_order:wx_order

echo "begin to truncate the table f_order_wx_order everyday"
hadoop fs -rm -r  "hdfs://hdp-0.local:8020/path/dbsync/f_order_wx_order"
echo "truncate table f_order_wx_order done!!"

echo "begin to load the data of wx_order from f_order db to hive  dbsync.f_order_wx_order"
sqoop import --connect jdbc:mysql://192.168.11.82:3308/f_order?tinyInt1isBit=false --username vp_f_read --password vSYaTqhjmtJ+l7ix  -m 4  --hive-overwrite  --table wx_order  --map-column-hive vip_level=STRING,status=STRING,pay_type=STRING,open_vip_type=STRING,is_auto_renew=STRING,is_renewal=STRING,from_site=STRING,use_coupon=STRING,origin=STRING  --hive-database dbsync --hive-table  f_order_wx_order  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/f_order_wx_order' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'
echo "load  the data wx_order in f_order done!!"

# 8-2 f_order.wx_order_goods
echo "begin to truncate the table f_order_wx_order_goods everyday"
hadoop fs -rm -r  "hdfs://hdp-0.local:8020/path/dbsync/f_order_wx_order_goods"
echo "truncate table f_order_wx_order_goods done!!"

echo "begin to load the data of wx_order_goods from f_order db to hive  dbsync.f_order_wx_order_goods"
sqoop import --connect jdbc:mysql://192.168.11.82:3308/f_order?tinyInt1isBit=false --username vp_f_read --password vSYaTqhjmtJ+l7ix  -m 4  --hive-overwrite  --table wx_order_goods  --map-column-hive type=STRING,pay_type=STRING,open_vip_type=STRING,vip_level=STRING   --hive-database dbsync --hive-table  f_order_wx_order_goods  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/f_order_wx_order_goods' --hive-drop-import-delims --fields-terminated-by '\001'  --lines-terminated-by '\n'
echo "load  the data wx_order_goods in f_order done!!"




# 9 cbs_local_main: order_channel_info

echo "begin to truncate the dbsync.cbs_local_main_order_channel_info "
hive -e "truncate table dbsync.cbs_local_main_order_channel_info"


echo "begin to import the order_channel_info"

sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_local_main?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4 --hive-overwrite --table order_channel_info  --columns 'order_id, channel_type' --split-by order_id --hcatalog-database dbsync  --hcatalog-table cbs_local_main_order_channel_info --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

echo "import finished!!!"


# 10 filmora_uuid:wx_member_*

echo "begin to truncate the dbsync.filmora_uuid_wx_member "
hive -e "truncate table dbsync.filmora_uuid_wx_member"

echo "begin to import the wx_member"
sqoop import --connect jdbc:mysql://192.168.11.82:3306/filmora_uuid?characterEncoding=utf-8 --username read_uuid --password wsdb123  -m 4  --hive-overwrite --query 'select t1.uid,t1.email,t1.username,t1.language,t1.register_platform,t1.register_time,t1.activated_platform,t1.activated_platform_bit,t1.license,t1.effects_paid,t1.subscribed_es,t1.download_free_es,t1.is_old_member,t1.last_visit,t1.inputtime,t1.updatetime,t1.reg_country,t1.from_brand,t1.edm_subscribe_status,t1.from_site from (SELECT * FROM wx_member_0 union SELECT * FROM wx_member_1    union   SELECT * FROM wx_member_2    union   SELECT * FROM wx_member_3    union   SELECT * FROM wx_member_4    union   SELECT * FROM wx_member_5    union   SELECT * FROM wx_member_6    union   SELECT * FROM wx_member_7    union   SELECT * FROM wx_member_8    union   SELECT * FROM wx_member_9) t1  WHERE $CONDITIONS and (t1.uid<99999999999)'   --split-by t1.uid --hcatalog-database dbsync  --hcatalog-table filmora_uuid_wx_member --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose


echo "import finished!!!"


#  11 cbs: product_detail

echo "begin to truncate the dbsync.cbs_out_product_detail "
hive -e "truncate table dbsync.cbs_out_product_detail"
echo "begin to import the product_detail"
sqoop import --connect jdbc:mysql://192.168.10.69:3306/cbs_out?characterEncoding=utf-8 --username vpbigdata --password HHzp##94395  -m 4  --hive-overwrite  --query " select * from (select
t1.product_id,
t1.name,
t3.brand_name,
t2.language_name,
case
	when left(t1.product_num,1) ='1' then 'Win'
	when left(t1.product_num,1) ='2' then 'Mac'
	when left(t1.product_num,1) ='3' then 'Mobile'
	when left(t1.product_num,1) ='5' then '其他'
else '废弃'
end  platform,
case
	when t4.category_name='Selfie' then 'Effect Store'
	when t4.category_name='Video Editor' then 'Filmora'
else t4.category_name
end  category_name
from product_info as  t1
join
sys_language as t2
on t1.language_id = t2.language_id
join
sys_brand as  t3
on t1.brand_id= t3.brand_id
join
product_type as t4
on t1.category_id = t4.category_id) t WHERE \$CONDITIONS "  --split-by t.product_id  --hcatalog-database dbsync  --hcatalog-table cbs_out_product_detail --hive-overwrite --null-string '\\N' --null-non-string '\\N' --verbose

# 12 base_waelog
# 导入下载器数据到hive仓库
echo "begin to import the waelog"

hadoop fs -rm -r  hdfs://hdp-0.local:8020/path/dbsync/base_waelog
sqoop import    --connect jdbc:mysql://192.168.9.88:3306/was_base?characterEncoding=utf-8 --username vpbigdata --password vp#12345  -m 4  --hive-overwrite  --query "select * from (SELECT
client_sign,
product_id,
downstate,
installstate,
token,
date(date) as day
from waelog where token is not null ) t WHERE \$CONDITIONS   " --split-by t.product_id  --hive-database dbsync      --hive-table base_waelog --hive-import  --target-dir  'hdfs://hdp-0.local:8020/path/dbsync/base_waelog'




echo "import finished!!!"
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s"







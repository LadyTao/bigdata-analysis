#!/bin/bash

echo '清理mysql的数据data_sale.ci_order'
mysql  -h 10.14.1.10 -P 3306 -u root -pws2018 -Ddata_sale -AB -e "truncate table ci_order;" #导入数据到mysql之前，需要先清空相关表的数据
echo "mysql数据清理完毕"

echo "开始导入订单相关数据到data_sale.ci_order"
sqoop export   --connect jdbc:mysql://10.14.1.10:3306/data_sale   --username root  --password ws2018  -m 8 --table ci_order   --export-dir 'hdfs://hdp-0.local:8020/path/mart/ci_order'  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'
echo "完成数据导入data_sale.ci_order"


#--导入相关数据到mysql前需要先清理mysql数据
echo '清理mysql的数据data_user.ci_member_renew_rate_day中需要更新的数据'
expire=`date -d "-7 day "  +"%Y-%m-%d"`
mysql  -h 10.14.1.10 -P 3306 -u root -pws2018 -Ddata_user -AB -e "drop from data_user.ci_member_renew_rate_day where stat_date>=$expire; " #导入数据到mysql之前，需要先清空相关表的数据
echo "mysql数据清理完毕"


# 查询需要更新的分区数据据，写入mart.ci_member_renew_rate_day ,
hive -e "insert overwrite table mart.ci_member_renew_rate_day select * from mart.ci_member_renew_rate_day1 where expire_date>=$expire;"


echo "开始导入相关续费数据到data_user.ci_member_renew_rate_day"
sqoop export   --connect jdbc:mysql://10.14.1.10:3306/data_user   --username root  --password ws2018  -m 8 --table ci_member_renew_rate_day   --export-dir 'hdfs://hdp-0.local:8020/path/mart/ci_member_renew_rate_day1'  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'

echo "完成数据导入data_user.ci_member_renew_rate_day1"
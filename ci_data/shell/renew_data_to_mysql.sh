
#!/bin/bash

echo '清理mysql的数据data_sale.ci_order'
mysql  -h 10.14.1.10 -P 3306 -u root -pws2018 -Ddata_sale -AB -e "truncate table ci_order;" #导入数据到mysql之前，需要先清空相关表的数据
echo "mysql数据清理完毕"

echo "开始导入订单相关数据到data_sale.ci_order"
sqoop export   --connect jdbc:mysql://10.14.1.10:3306/data_sale   --username root  --password ws2018  -m 8 --table ci_order   --export-dir 'hdfs://hdp-0.local:8020/path/mart/ci_order'  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'
echo "完成数据导入data_sale.ci_order"


#--导入相关数据到mysql前需要先清理mysql数据
echo '清理mysql的数据data_user.ci_member_renew_rate_day'
mysql  -h 10.14.1.10 -P 3306 -u root -pws2018 -Ddata_user -AB -e "truncate table ci_member_renew_rate_day;" #导入数据到mysql之前，需要先清空相关表的数据
echo "mysql数据清理完毕"

echo "开始导入相关续费数据到data_user.ci_member_renew_rate_day"
# 分区表
#first_date="2019-01-11"
#expire=`date  +"%Y-%m-%d"`
#while [[ $first_date < $expire  ]]
#do
        #echo $first_date
                #hive -hiveconf expire=$first_date -f   /usr/local/bigdata/jobtaskh0/shelljob/ci_data/ci_renew_day.sql
         #        sqoop export   --connect jdbc:mysql://10.14.1.10:3306/data_user   --username root  --password ws2018   --table ci_member_renew_rate_day  --hive-partition-key expire_date --hive-partition-value $first_date --export-dir "hdfs://hdp-0.local:8020/path/mart/ci_member_renew_rate_day/expire_date=$first_date"  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'

        #first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
#done
# 普通表,使用分区表时，导hive表数据到mysql 消耗时间过长，故在hive中改用普通表，不分区
sqoop export   --connect jdbc:mysql://10.14.1.10:3306/data_user   --username root  --password ws2018  -m 8 --table ci_member_renew_rate_day   --export-dir 'hdfs://hdp-0.local:8020/path/mart/ci_member_renew_rate_day1'  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'

echo "完成数据导入data_user.ci_member_renew_rate_day1"
#!/bin/bash

#  move the renew data from hive to mysql
first_date="2019-01-11"
expire=`date +"%Y-%m-%d"`
end_date=`date -d "8 days"  +"%Y-%m-%d"`
while [[ $first_date < $end_date  ]]
do
        echo $first_date
		#hive -hiveconf expire=$first_date -f   /usr/local/bigdata/jobtaskh0/shelljob/ci_data/ci_renew_day.sql
		 sqoop export   --connect jdbc:mysql://10.14.1.102:3306/data_user   --username root  --password testlalatest   --table ci_member_renew_rate_day  --hive-partition-key expire_date --hive-partition-value $first_date --export-dir "hdfs://hdp-0.local:8020/path/mart/ci_member_renew_rate_day/expire_date=$first_date"  --input-fields-terminated-by '\001'   --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N'
        first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
done




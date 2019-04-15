#!/bin/bash

#sudo su hdfs

filePath=/usr/local/bigdata/jobtaskh0/shelljob/ci_data/



starttime=`date +'%Y-%m-%d %H:%M:%S'`

echo "begin to delete the old data in the mart db"


echo "begin to insert the ci_order data into mart.ci_order"
hive  -f $filePath"/ci_order.sql"
echo "done"
#执行程序
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s"

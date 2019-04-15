#!/bin/bash

#sudo su hdfs

filePath=/home/hdfs/



starttime=`date +'%Y-%m-%d %H:%M:%S'`

echo "begin to delete the old data in the mart db"


echo "begin to insert data to mart"
hive  -f $filePath"/martHive.sql"
echo "done"
#执行程序
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s"


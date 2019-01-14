
#!/bin/bash

#sudo su hdfs

filePath=/home/hdfs//usr/local/bigdata/jobtaskh0/shelljob/ci_data/



starttime=`date +'%Y-%m-%d %H:%M:%S'`

echo "begin to delete the old data in the mart db"


echo "begin to insert the ci_order data into mart.ci_order"
hive  -f $filePath"/ci_order.sql"
echo "done"
#ִ�г���
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "��������ʱ�䣺 "$((end_seconds-start_seconds))"s"

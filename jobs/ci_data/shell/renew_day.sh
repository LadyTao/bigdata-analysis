#!/bin/bash

# calculate the expire and renew users info  and insert the result to mart.ci_member_renew_rate_day in hive;


starttime=`date +'%Y-%m-%d %H:%M:%S'`

#hive -e " truncate table mart.ci_member_renew_rate_day1;"
first_date="2019-01-11"
expire=`date  +"%Y-%m-%d"`


while [[ $first_date < $expire ]]
do
	end=`date -d  "$expire" +%s`
	begin=`date -d  "$first_date" +%s`
	stampDiff=`expr $end - $begin`
	datediff=`expr $stampDiff / 86400`
	if [ $datediff -gt 7 ]; then
			echo 'no need to calculate the day:' $first_date
			first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
	else
		echo 'calculate the day:'+$first_date
		#hive -hiveconf expire=$first_date -f   /usr/local/bigdata/jobtaskh0/shelljob/ci_data/ci_renew_day.sql
		echo 'hive calculate'
		first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
	fi

done

echo "done"
#执行程序
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s"

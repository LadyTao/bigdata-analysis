#!/bin/bash

# calculate the expire and renew users info  and insert the result to mart.ci_member_renew_rate_day in hive;

first_date="2019-01-11"
expire=`date +"%Y-%m-%d"`
end_date=`date -d "8 days"  +"%Y-%m-%d"`
while [[ $first_date < $end_date  ]]
do
        echo $first_date
		hive -hiveconf expire=$first_date -f   /usr/local/bigdata/jobtaskh0/shelljob/ci_data/ci_renew_day.sql
        first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
done

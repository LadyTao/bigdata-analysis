#!/bin/bash

# calculate the expire and renew users info  and insert the result to mart.ci_member_renew_rate_day in hive;
hive -e " truncate table mart.ci_member_renew_rate_day1;"
first_date="2019-01-11"
expire=`date  +"%Y-%m-%d"`
while [[ $first_date < $expire ]]
do
        echo $first_date
                hive -hiveconf expire=$first_date -f   /usr/local/bigdata/jobtaskh0/shelljob/ci_data/ci_renew_day.sql
        first_date=`date -d "+1 day $first_date"  +"%Y-%m-%d"`
done

~

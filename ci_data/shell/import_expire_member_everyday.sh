#!/bin/bash

expire_date=`date  +'%Y-%m-%d'`
#expire_date=`date -d "1 days"  +"%Y-%m-%d"`


hive<<EOF

use dbsync;insert into dbsync.ci_wx_member_expire_date partition(expireDate='$expire_date') select id,uid,vip_type,endtime,status,base_info, mobile,inputtime,updatetime,pay_mode from dbsync.shencut_store_wx_member where from_unixtime(endtime+28800,'yyyy-MM-dd')='$expire_date';

EOF

#!/bin/bash

# exec in the shell
# the shell can be run every hour ,the  new data in the tables  will be append to hive's table

echo 'append the  data int store.wx_member  to hive dbsync.shencut_store_wx_member'
sqoop job --exec  shencut_store_wx_member_add

echo 'append the  data int store.wx_order  to hive dbsync.shencut_store_wx_order_add '
sqoop job --exec  shencut_store_wx_order_add

echo 'append the  data int store.wx_member_recharge_record  to hive dbsync.shencut_store_wx_member_recharge_record'
sqoop job --exec  shencut_store_wx_member_recharge_record_add
# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import range
from datetime import timedelta, datetime

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': datetime(2019,01,22),
    'email': ['huanganqing@wondershare.cn', '2906368@qq.com','zhaopeng@wondershare.cn'],
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='ci_order_renew_statistic',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=120),
)


# [START howto_operator_bash_template]
#from airflow.contrib.hooks import ssh_hook as SSHHook
#from airflow.contrib.operators import SSHOperator as SSHExecuteOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

#sshHook = SSHHook(conn_id="ws@hdp-0")

#import_sh = SSHExecuteOperator(
#    task_id="import_sh",
#    bash_command="/home/ws/airflow/order_uuid_analytics/import.sh",
#    ssh_hook=sshHook,
#    dag=dag)


# 每日增量导入订单，充值数据，同步全量导入会员数据 建议执行时间每日0时0分
import_ci_hive = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='import_ci_hive',
    #command='/home/ws/airflow/order_uuid_analytics/import.sh ', # 在remote机器/tmp/下面创建一个文件
    command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/import_ci_hive.sh > /usr/local/bigdata/logs/shelljob/ci_data/ci_load.log 2>&1 & ', # 在remote机器/tmp/下面创建一个文件
    dag=dag
)


# 同步一份用户当日到期用户的数据到相关分区表，用于用户到期状态快照保存
import_expire_member_everyday_sh = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='import_expire_member_everyday',
    #command='/home/ws/airflow/order_uuid_analytics/import.sh ', # 在remote机器/tmp/下面创建一个文件
    command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/import_expire_member_everyday.sh > /usr/local/bigdata/logs/shelljob/ci_data/member_expire.log 2>&1 & ', # 在remote机器/tmp/下面创建一个文件
    dag=dag
)


# 计算相关订单数据，计算结果落地hive数仓 mart.ci_order 表
insert_ci_order_sh = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='insert_ci_order_sh',
    command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/insert_ci_order.sh  > /usr/local/bigdata/logs/shelljob/ci_data/insert_ci_order.log 2>&1 & ',
    dag=dag
)



sleep_5min = BashOperator(
    task_id='sleep_5min',
    bash_command='sleep 1',
    retries=3,
    dag=dag
)

# 全量dump  mart.ci_order 表数据到ES的 ci index

dumper_ci_order_to_es = BashOperator(
#     task_id='dump_order_into_es',
#     bash_command='/home/ws/airflow/order_uuid_analytics/venv/bin/python3 /home/ws/airflow/order_uuid_analytics/dump_order.py',
#     dag=dag,
# )
#



#每日到期用户数和续费用户数计算脚本执行命令,计算结果落地hive数仓 mart.ci_member_renew_rate_day表
renew_day_sh = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='renew_day_sh',
    command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/renew_day.sh  > /usr/local/bigdata/logs/shelljob/ci_data/renew_day.log 2>&1 & ',
    dag=dag
)

#导入hive数仓 mart.ci_member_renew_rate_day表中每日的到期用户数和续费用户数到mysql数据库，供计算接口使用
renew_data_to_mysql_sh = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='renew_data_to_mysql_sh',
    command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/renew_data_to_mysql.sh  > /usr/local/bigdata/logs/shelljob/ci_data/renew_data_to_mysql.log 2>&1 & ',
    dag=dag
)

import_ci_hive >> import_expire_member_everyday_sh
import_expire_member_everyday_sh >> insert_ci_order_sh
insert_ci_order_sh >> sleep_5min

sleep_5min >> dumper_ci_order_to_es
dumper_ci_order_to_es >> renew_day_sh
renew_day_sh >> renew_data_to_mysql_sh

if __name__ == "__main__":
    dag.cli()
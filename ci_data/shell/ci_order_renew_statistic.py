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
    'start_date': datetime(2019, 1, 22),
    'email': ['huanganqing@wondershare.cn', '2906368@qq.com',
              'zhaopeng@wondershare.cn'],
    'depends_on_past': False,
    'catchup': False
}

dag = DAG(
    dag_id='ci_order_renew_statistic',
    default_args=args,
    schedule_interval='5 1 * * *',
    dagrun_timeout=timedelta(minutes=120),
)

# 每日增量导入订单，充值数据，同步全量导入会员数据 建议执行时间每日0时0分
import_ci_hive = BashOperator(
    # ssh_conn_id='ws@hdp-0',
    task_id='ci_import_ci_hive',
    # command='/home/ws/airflow/order_uuid_analytics/import.sh ', # 在remote机器/tmp/下面创建一个文件
    bash_command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/import_ci_hive.sh  ',
    # 在remote机器/tmp/下面创建一个文件
    dag=dag,
    run_as_user='hdfs'
)

# 同步一份用户当日到期用户的数据到相关分区表，用于用户到期状态快照保存
import_expire_member_everyday_sh = BashOperator(
    # ssh_conn_id='ws@hdp-0',
    task_id='ci_import_expire_member_everyday',
    # command='/home/ws/airflow/order_uuid_analytics/import.sh ', # 在remote机器/tmp/下面创建一个文件
    bash_command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/import_expire_member_everyday.sh  ',
    # 在remote机器/tmp/下面创建一个文件
    dag=dag,
    run_as_user='hdfs'
)

# 计算相关订单数据，计算结果落地hive数仓 mart.ci_order 表
insert_ci_order_sh = BashOperator(
    # ssh_conn_id='ws@hdp-0',
    task_id='ci_insert_ci_order_sh',
    bash_command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/insert_ci_order.sh   ',
    dag=dag,
    run_as_user='hdfs'
)

sleep_5min = BashOperator(
    task_id='sleep_5min',
    bash_command='sleep 1',
    retries=3,
    dag=dag
)

# 全量dump  mart.ci_order 表数据到ES的 ci index

dumper_ci_order_to_es = BashOperator(
    task_id='ci_dump_order_into_es',
    bash_command='/home/ws/projects/bigdata-report/pythonjob/venv/bin/python3 /home/ws/projects/bigdata-report/pythonjob/es_dump/ci/dump_order.py',
    dag=dag
)

# 每日到期用户数和续费用户数计算脚本执行命令,计算结果落地hive数仓 mart.ci_member_renew_rate_day表
renew_day_sh = BashOperator(
    task_id='ci_renew_day_sh',
    bash_command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/renew_day.sh   ',
    dag=dag,
    run_as_user='hdfs'
)

# 导入hive数仓 mart.ci_member_renew_rate_day表中每日的到期用户数和续费用户数到mysql数据库，供计算接口使用
renew_data_to_mysql_sh = BashOperator(
    task_id='ci_renew_data_to_mysql_sh',
    bash_command='bash /usr/local/bigdata/jobtaskh0/shelljob/ci_data/renew_data_to_mysql.sh  ',
    dag=dag,
    run_as_user='hdfs'
)

import_ci_hive >> import_expire_member_everyday_sh
import_expire_member_everyday_sh >> insert_ci_order_sh
insert_ci_order_sh >> sleep_5min

sleep_5min >> dumper_ci_order_to_es
dumper_ci_order_to_es >> renew_day_sh
renew_day_sh >> renew_data_to_mysql_sh

if __name__ == "__main__":
    dag.cli()

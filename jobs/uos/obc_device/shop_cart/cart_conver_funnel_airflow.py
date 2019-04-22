# -*- coding: utf-8 -*-
"""

此文件为airflow调度文件,主要是调度相关pyspark任务
使用此文件时，需要修改如下内容：
1  airflow_pyspark_template.py 修改为相关pyspark 任务的名称
2   修改args中，相关的个人信息，包括owner,email
3   根据任务时间类型选择相关的dag，同时注意修改相关的scheduler_interval,其格式参数可参考crontab
4   修改相关task_id 为可读的名称，同时修改pyspark*.py的文件路径 和任务的时间类型
5   假设此时任务为 airflow_uos_label.py ，dag_name: airflow_uos_label  task_id:insert 是按天执行，及每天执行昨天的数据，做相关操作。
6   切换目录到hdp-0:/home/ws/airflow
7   执行 source venv/bin/activate  &&  cd dags  &&  vim aiflow_uos_label.py 粘贴相关内容
8   测试任务是否编写成功：使用命令格式如：  airflow test dag_name task_id date 针对该任务即为： airflow test airflow_uos_label insert 2019-04-02
"""

from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

args = {
    'owner': 'shiTao',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email': ['shitao@wondershare.cn'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# day 类型的任务 根据相应的类型，打开或者关闭相关的注释
dag = DAG(
    dag_id='cart_conver_funnel',
    default_args=args,
    schedule_interval='0 4 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

# # week 类型的任务
# dag = DAG(
#     dag_id='airflow_pyspark_template_week',
#     default_args=args,
#     schedule_interval='50 6 * * 1',
#     dagrun_timeout=timedelta(minutes=60),
# )
#
#
# # month 类型的任务 dag_id 需要修改
# dag = DAG(
#     dag_id='airflow_pyspark_template_week',
#     default_args=args,
#     schedule_interval='50 2 1 * *',
#     dagrun_timeout=timedelta(minutes=60),
# )
# task_id也需要修改为相应的任务描述
day_partition = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='day_partition',
    command=" cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/uos/uid_label/cart_conver_funnel.py  \
                day \
                {{ ds_nodash }} ",
    dag=dag
)
if __name__ == "__main__":
    dag.cli()

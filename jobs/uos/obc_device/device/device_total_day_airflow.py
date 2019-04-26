# -*- coding: utf-8 -*-


from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

args = {
    'owner': 'mayx',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email': ['mayx@wondershare.cn'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# day 类型的任务 根据相应的类型，打开或者关闭相关的注释
dag = DAG(
    dag_id='device_total_day',
    default_args=args,
    schedule_interval='30 5 * * *',
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
device_active_first = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='device_total',
    command=" cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/uos/device/device_total_day.py  \
                day \
                {{ ds_nodash }} ",
    dag=dag
)


if __name__ == "__main__":
    dag.cli()

# -*- coding: utf-8 -*-


from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator

args = {
    'owner': 'zhaopeng',
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email': ['zhaopeng@wondershare.cn'],
    'email_on_failure': True,
    'email_on_retry': True,
}


# # day 类型的任务 根据相应的类型，打开或者关闭相关的注释
# dag = DAG(
#     dag_id='pyspark_run_template',
#     default_args=args,
#     schedule_interval='50 6 * * *',
#     dagrun_timeout=timedelta(minutes=60),
# )
#





# # week 类型的任务
# dag = DAG(
#     dag_id='pyspark_run_template',
#     default_args=args,
#     schedule_interval='50 6 * * 1',
#     dagrun_timeout=timedelta(minutes=60),
# )
#

# month 类型的任务
dag = DAG(
    dag_id='pyspark_run_template',
    default_args=args,
    schedule_interval='50 6 1 * *',
    dagrun_timeout=timedelta(minutes=60),
)

insert = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='pyspark_template_day',
    command=" cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/pyspark_template.py \
                day \
                {{ ds_nodash }} ",
    dag=dag
)

if __name__ == "__main__":
    dag.cli()

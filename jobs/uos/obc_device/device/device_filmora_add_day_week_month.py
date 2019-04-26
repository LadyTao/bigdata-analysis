# -*- coding: utf-8 -*-
# 设备新增指标数据 增量计算调度任务
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


args = {
    'owner': 'zhaopeng',
    'start_date': datetime(2019, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=30),
    'email': ['zhaopeng@wondershare.cn'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    dag_id='device_filmora_add_day_week_month',
    default_args=args,
    schedule_interval='50 5 * * *',
    dagrun_timeout=timedelta(minutes=60),
)


daily = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='daily',
    command='cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/uos/device/device_add_day.py  \
                day \
                {{ ds }} ',
    dag=dag
)

weekly = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='weekly',
    command='cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/uos/device/device_add_week.py  \
                week \
                {{ ds }} ',
    dag=dag
)

monthly = SSHOperator(
    ssh_conn_id='ws@hdp-0',
    task_id='monthly',
    command='cd /usr/local/bigdata/jobtaskh0/pythonjob/pyspark_template/ && spark-submit \
                --num-executors 4 \
                --executor-memory 4G \
                --executor-cores 4 \
                --driver-memory 4G \
                --driver-cores 4 \
                --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
                /usr/local/bigdata/jobtaskh0/pythonjob/uos/device/device_add_month.py  \
                month \
                {{ ds }} ',
    dag=dag
)


def judge_if_1st_day_of_week(**kwargs):
    print(kwargs.get('ds'))
    ds = datetime.strptime(kwargs.get('ds'), "%Y-%M-%d")
    day_of_week = ds.isoweekday()
    #  Monday; 1, ...
    if day_of_week == 1:
        logging.warn('judge_if_1st_day_of_week: Monday')
        return "weekly"
    logging.warn('judge_if_1st_day_of_week: NOT Monday')
    return "if_1st_day_of_month"

def judge_if_1st_day_of_month(**kwargs):
    print(kwargs.get('ds'))
    if kwargs.get('ds').endswith('01'):
        logging.warn('judge_if_1st_day_of_month: 01' + kwargs.get('ds'))
        return True
    logging.warn('judge_if_1st_day_of_week: Not 01' + kwargs.get('ds'))
    return False

if_1st_day_of_week = BranchPythonOperator(
    task_id='if_1st_day_of_week',
    python_callable=judge_if_1st_day_of_week,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag)

if_1st_day_of_month = ShortCircuitOperator(
    task_id='if_1st_day_of_month',
    python_callable=judge_if_1st_day_of_month,
    provide_context=True,
    trigger_rule="all_done",
    dag=dag)


daily >> if_1st_day_of_week
if_1st_day_of_week >> if_1st_day_of_month
if_1st_day_of_week >> weekly
weekly >> if_1st_day_of_month
if_1st_day_of_month >> monthly




if __name__ == "__main__":
    dag.cli()
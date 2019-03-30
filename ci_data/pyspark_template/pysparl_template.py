# -*- coding: UTF-8 -*-

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pprintpp import pprint as pp

import pyspark.sql.functions as F

import datetime

"""
spark-submit pyspark  file with special resources demo

./bin/spark-submit    --num-executors 4 --executor-memory 4G --executor-cores 4 --driver-memory 4G --driver-cores 4 path/somefile.py


 插入mysql 时一定要添加  --jars  --driver-class-path  
./bin/spark-submit  --num-executors 4 --executor-memory 4G --executor-cores 4 --driver-memory 4G --driver-cores 4 --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar  /usr/local/bigdata/jobtaskh0/pythonjob/uos/uid_label/test.py day '2019-03-28' 



"""


def excute(sql_query, sql_session):
    """
        执行相关sql查询,展示20条数据
    :params sql_query: select cluase
    :params sql_session:  SparkSession
    """
    df = sql_session.sql(sql_query)
    df.show()


def insert_into_mysql(sql_query, sql_session):
    """
    把聚合的数据查询相关的mysql中

    :params sql_query: insert cluase
    :params sql_session:  SparkSession

    """
    print("begin to insert data to mysql")
    sql_query
    df = sql_session.sql(sql_query)

    # Saving data to a JDBC source

    df.write.format("jdbc") \
        .option("url", "jdbc:mysql://10.14.1.10:3306/data_user") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "uos_com_tags_all") \
        .option("user", "root") \
        .option("password", "ws2018") \
        .save(mode="overwrite")
    print("insert to mysql  ok ")


def insert_to_hive(sql_query, sql_session):
    """
    此处可以insert语句直接写到相关sql中，执行完sql ,直接将相关的数据插入到hive中的相表。
    :params sql_query: insert cluase
    :params sql_session:  SparkSession
    """
    print("begin to insert data to hive")
    df = sql_session.sql(sql_query)
    print("insert into hive table done")




def day(date, moudle_sql):
    """

    :param date: '2019-03-30'
    :param moudle_sql:
    :return:
    """

    date = (datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(
        days=1)).strftime('%Y-%m-%d')
    moudle_sql = moudle_sql.replace('__DAY1__', str(date)).replace('__DAY2__',
                                                                   str(date))

    return moudle_sql


def week(date, moudle_sql):
    """

    :param date: '2019-03-30'
    :param moudle_sql:
    :return:
    """

    date_start = (datetime.datetime.strptime(date,
                                             '%Y-%m-%d') - datetime.timedelta(
        days=7)).strftime('%Y-%m-%d')
    date_end = (datetime.datetime.strptime(date,
                                           '%Y-%m-%d') - datetime.timedelta(
        days=1)).strftime('%Y-%m-%d')

    moudle_sql = moudle_sql.replace('__DAY1__', str(date_start)).replace(
        '__DAY2__', str(date_end))
    return moudle_sql


def month(date, moudle_sql):
    """

    :param date: '2019-03-30'
    :param moudle_sql:
    :return:
    """

    tmp_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    print(tmp_date)
    date_start = datetime.datetime(tmp_date.year, tmp_date.month - 1,
                                   1).strftime('%Y-%m-%d')
    date_end = (datetime.datetime(tmp_date.year, tmp_date.month,
                                  1) - datetime.timedelta(1)).strftime(
        '%Y-%m-%d')
    moudle_sql = moudle_sql.replace('__DAY1__', str(date_start)).replace(
        '__DAY2__', str(date_end))
    return moudle_sql





if __name__ == "__main__":
    SparkContext.setSystemProperty("hive.metastore.uris", "thrift://hdp-0:9083")

    sparksession = (SparkSession
                    .builder
                    .appName('select_insert_mysql_hive')
                    .enableHiveSupport()
                    .getOrCreate())




    moudle_sql = """
     select 
    t1.wsid,
    max(t1.ever_export_youtube_vimeo) as ever_export_youtube_vimeo
    from
    (
    select   wsid ,
    case when upper(params["export_type"]) IN ("YOUTUBE","VIMEO")  then 1 else 0 end as ever_export_youtube_vimeo
    from log.cl_filmora_win where  day  between    '__DAY1__' and '__DAY2__'  
    and wsid is not null and logtype="export" and logitem="export_format" and params['export_type'] in ("YouTube","Vimeo") 
    
    ) t1
    group by t1.wsid
    union all 
    select 
    t2.wsid,
    max(t2.ever_export_youtube_vimeo) as ever_export_youtube_vimeo
    from
    (
    select   wsid ,
    case when upper(params["export_type"]) IN ("YOUTUBE","VIMEO")  then 1 else 0 end as ever_export_youtube_vimeo
    from log.cl_filmora_mac where day  between    '__DAY1__' and '__DAY2__'  
    and wsid is not null and logtype="export" and logitem="export_format" and params['export_type'] in ("YouTube","Vimeo") 
    
    ) t2
    group by t2.wsid

    """



    # spark-submit 提供时间类型参数，计算日，周，月 day,week,month
    time_type = sys.argv[1]

    excute_date = str(sys.argv[2])

    # 根据任务传入的时间类型，判断执行的任务是日任务，周任务或者月任务
    if time_type == 'day':
        sql_str = day(date=excute_date, moudle_sql=moudle_sql)
        print("sql_str:", sql_str)
        excute(sql_query=sql_str, sql_session=sparksession)
    elif time_type == 'week':
        sql_str = week(date=excute_date, moudle_sql=moudle_sql)
        print("sql_str:", sql_str)
        excute(sql_query=sql_str, sql_session=sparksession)
    else:
        sql_str = month(date=excute_date, moudle_sql=moudle_sql)
        print("sql_str:", sql_str)
        excute(sql_query=sql_str, sql_session=sparksession)


    sparksession.stop()

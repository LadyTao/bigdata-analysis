# -*- coding: UTF-8 -*-
# 设备新增月指标增量计算代码
import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pprintpp import pprint as pp

import pyspark.sql.functions as F

import datetime
import calendar
import argparse
from dateutil.parser import parse


def excute(sql_query, sql_session):
    """
        执行相关sql查询,展示20条数据
    :params sql_query: select cluase
    :params sql_session:  SparkSession
    """
    df = sql_session.sql(sql_query)
    df.show()


def insert_into_mysql(sql_query, sql_session, table):
    """
    把聚合的数据查询相关的mysql中

    :params sql_query: insert cluase
    :params sql_session:  SparkSession

    """
    print("begin to insert data to mysql")
    print("sql_query", sql_query)
    df = sql_session.sql(sql_query)

    # Saving data to a JDBC source

    df.write.format("jdbc") \
        .option("url", "jdbc:mysql://10.14.1.10:3306/data_user") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", table) \
        .option("user", "root") \
        .option("password", "ws2018") \
        .save(mode="append")
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


def job_day(date, moudle_sql):
    """
     利用调度日期参数，替换model_sql的日期参数，替换后的module_sql作为执行sql,传入相关的sparksession,执行相关的日任务
    :param date:job_day: '2019-03-30' 一般为每天，获取相关的执行周期数据，day类型的任务一般为：该天
    :param moudle_sql:
    :return: model_sql
    Examples
    --------
    usage:

    # >>> module_sql= ' select * from  db.table where day between __DAY1__ and __DAY2__ '
    # >>> jod_date = '2019-03-25'
    # >>> excute_sql = job_day(date= job_date,module_sql= module_sql)
    # >>> print("excute_sql:",excute_sql)
    "excute_sql":  " select * from  db.table where day between '2019-03-25' and '2019-03-25' "

    """

    date = date.strftime('%Y-%m-%d')

    moudle_sql = moudle_sql.replace('__DAY1__', str(date)).replace('__DAY2__',
                                                                   str(date))

    return moudle_sql


def job_week(date, moudle_sql):
    """
    利用调度日期参数，替换model_sql的日期参数，替换后的module_sql作为执行sql,传入相关的sparksession,执行相关的周任务
    :param date: job_week ：'2019-03-25' 一般为每周的周一，获取相关的执行周期数据，week类型的任务一般为：周一到该周的周日
    :param moudle_sql:
    :return:modeule_sql:excute_sql

     Examples
    --------
    usage:

    # >>> module_sql= ' select * from  db.table where day between __DAY1__ and __DAY2__ '
    # >>> jod_date = '2019-03-25'
    # >>> excute_sql = job_day(date= job_date,module_sql= module_sql)
    # >>> print("excute_sql:",excute_sql)
    "excute_sql":  " select * from  db.table where day between '2019-03-25' and '2019-03-31' "
    """

    date_start = date.strftime('%Y-%m-%d')

    date_end = (date + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    moudle_sql = moudle_sql.replace('__DAY1__', str(date_start)).replace(
        '__DAY2__', str(date_end))
    return moudle_sql


def job_month(date, moudle_sql):
    """
    利用调度日期参数，替换model_sql的日期参数，替换后的module_sql作为执行sql,传入相关的sparksession,执行相关的月任务
    :param date: job_date:'2019-03-01' 一般为每月的第一天，month类型的任务一般为该月的第一天到最后一天。
    :param moudle_sql:moudle_sql
    :return:

    Examples
    --------
    usage:

    # >>> module_sql= ' select * from  db.table where day between __DAY1__ and __DAY2__ '
    # >>> jod_date = '2019-03-25'
    # >>> excute_sql = job_day(date= job_date,module_sql= module_sql)
    # >>> print("excute_sql:",excute_sql)
    "excute_sql":  " select * from  db.table where day between '2019-03-01' and '2019-03-31' "
    """

    date_start = date.strftime('%Y-%m-%d')
    monthrange = calendar.monthrange(date.year, date.month)[1]
    date_end = (datetime.datetime(date.year, date.month,
                                  1) + datetime.timedelta(
        monthrange - 1)).strftime('%Y-%m-%d')
    moudle_sql = moudle_sql.replace('__DAY1__', str(date_start)).replace(
        '__DAY2__', str(date_end))
    return moudle_sql


if __name__ == "__main__":
    SparkContext.setSystemProperty("hive.metastore.uris", "thrift://hdp-0:9083")

    sparksession = (SparkSession
                    .builder
                    .appName('device_increase_add_month')
                    .enableHiveSupport()
                    .getOrCreate())

    # spark-submit 提供时间类型参数，计算日，周，月 day,week,month
    parser = argparse.ArgumentParser()
    parser.add_argument('time_type')
    parser.add_argument('excute_day')  # add_argument()指定程序可以接受的命令行选项
    args = parser.parse_args()
    time_type = args.time_type
    excute_date = parse(str(args.excute_day))

    print("begin to add data to device_increase_month")
    increase_month_sql = """
   select
    t1.*
    from
    (
        select
        month.stat_date,
        concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
       'shencut_win' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(first_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase                                                 
        from
       base.device_shencut_win where   day= '__DAY1__'  and first_active  between  trunc(add_months('__DAY1__' ,-1),'MM') and  date_sub('__DAY1__',1) AND app_version is not null
        group by
        trunc(first_active, 'MM'),  
        app_version
        ) month  order by month.stat_date desc
    ) t1
    union all
    select 
	t2.*
    from
    (
        select
        month.stat_date,
        concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
       'shencut_mac' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(first_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase
        from
       base.device_shencut_mac where   day= '__DAY1__' and first_active  between  trunc(add_months('__DAY1__' ,-1),'MM') and  date_sub('__DAY1__',1) AND app_version is not null
        group by
        trunc(first_active, 'MM'),  
        app_version
        ) month  order by month.stat_date desc
    ) t2

    """
    if time_type == 'month':
        increase_month_sql = job_day(date=excute_date,
                                     moudle_sql=increase_month_sql)
        insert_into_mysql(sql_query=increase_month_sql,
                          sql_session=sparksession,
                          table="device_increase_month")

    print("begin to add data to device_active_month")
    active_month_sql = """
        select 
        t1.* 
        from
        (   select
        month.stat_date,
        concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
       'shencut_win' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(last_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase
        from
       base.device_shencut_win where   day= '__DAY1__'  AND last_active between trunc(add_months('__DAY1__' ,-1),'MM') and date_sub('__DAY1__',1) AND app_version is not null
        group by
        trunc(last_active, 'MM'),  
        app_version
        ) month  order by month.stat_date desc

        ) t1
        union all
        select 
        t2.*
        from
        (
           select
        month.stat_date,
        concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
       'shencut_mac' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(last_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase
        from
       base.device_shencut_mac where   day= '__DAY1__' AND last_active between trunc(add_months('__DAY1__' ,-1),'MM') and date_sub('__DAY1__',1) AND  app_version is not null
        group by
        trunc(last_active, 'MM'),  
        app_version
        ) month  order by month.stat_date desc

        ) t2

    """

    if time_type == 'month':
        active_month_sql = job_day(date=excute_date,
                                   moudle_sql=active_month_sql)
        insert_into_mysql(sql_query=active_month_sql, sql_session=sparksession,
                          table="device_active_month")
    sparksession.stop()

# -*- coding: UTF-8 -*-

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

"""
执行uos客户端行为标签表：
            uos_com.effect_usage，
            uos_com.screen,
            uos_com.total_export_times,
            uos_com.youtube_vimeo_label,
            base.member
            每日分区数据插入

 插入mysql 时一定要添加  --jars  --driver-class-path  任务时间类型参数：day,week,month 
./bin/spark-submit \
--num-executors 4 \
--executor-memory 4G \
--executor-cores 4 \
--driver-memory 4G \
--driver-cores 4 \
--jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
 --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar \
/usr/local/bigdata/jobtaskh0/pythonjob/uos/uid_label/device_active_pyspark.py \
day \
2019-03-28 

"""


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

    # 终端活跃数据，插入相关mysql
    sparksession = (SparkSession
                    .builder
                    .appName('device_active')
                    .enableHiveSupport()
                    .getOrCreate())

    # spark-submit 提供时间类型参数，计算日，周，月 day,week,month
    parser = argparse.ArgumentParser()
    parser.add_argument('time_type')
    parser.add_argument('excute_day')  # add_argument()指定程序可以接受的命令行选项
    args = parser.parse_args()
    time_type = args.time_type
    excute_date = parse(str(args.excute_day))

    # 根据任务传入的时间类型，判断执行的任务是日任务，周任务或者月任务
    # if time_type == 'day':
    #     sql_str = job_day(date=excute_date, moudle_sql=moudle_sql)
    #     print("sql_str:", sql_str)
    #     excute(sql_query=sql_str, sql_session=sparksession)
    # elif time_type == 'week':
    #     sql_str = job_week(date=excute_date, moudle_sql=moudle_sql)
    #     print("sql_str:", sql_str)
    #     excute(sql_query=sql_str, sql_session=sparksession)
    # else:
    #     sql_str = job_month(date=excute_date, moudle_sql=moudle_sql)
    #     print("sql_str:", sql_str)
    #     excute(sql_query=sql_str, sql_session=sparksession)

    day_sql = """
                select t1.*
                from 
                (select
                day.stat_date,
                day.stat_date as show_date,
                'filmora_win' as dev_type,
                day.app_version,
                day.increase
                from
                (
                select
                last_active as stat_date,
                -- date_sub(next_day(date_sub(day,1),'MO'),7) as stat_week,
                -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
                app_version ,
                count(devid) as increase
                from
                base.device_filmora_win where   day= '__DAY1__' AND app_version is not null
                group by
                last_active,
                app_version
                order by last_active desc
                ) day  order by day.stat_date desc	) t1
                union all	
                select t2.*
                from 
                (
                select
                day.stat_date,
                day.stat_date as show_date,
                'filmora_mac' as dev_type,
                day.app_version,
                day.increase
                from
                (
                select
                last_active as stat_date,
                -- date_sub(next_day(date_sub(day,1),'MO'),7) as stat_week,
                -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
                app_version ,
                count(devid) as increase
                from
                base.device_filmora_mac where   day= '__DAY1__' AND app_version is not null
                group by
                last_active,
                app_version
                order by last_active desc
                ) day  order by day.stat_date desc	
                ) t2
        """
    week_sql = """
            select
            t1.*
            from
            (
                    select
                week.stat_date,
                concat(week.stat_date,'~',date_add(week.stat_date,6)) as show_date,
                'filmora_win' as dev_type,
                week.app_version,
                week.increase
                from
                (
                select
                -- fisrt_active as stat_date,
                date_sub(next_day(last_active,'MO'),7) as stat_date, -- 周的第一天
                -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
                app_version   ,
                count(devid) as increase
                from
                base.device_filmora_win where   day= '__DAY1__' AND app_version is not null
                group by
                date_sub(next_day(last_active,'MO'),7),
                app_version
                ) week  order by week.stat_date desc
            ) t1
            union all 
            select 
            t2.* 
            from
            (        select
                week.stat_date,
                concat(week.stat_date,'~',date_add(week.stat_date,6)) as show_date,
                'filmora_mac' as dev_type,
                week.app_version,
                week.increase
                from
                (
                select
                -- fisrt_active as stat_date,
                date_sub(next_day(last_active,'MO'),7) as stat_date, -- 周的第一天
                -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
                app_version   ,
                count(devid) as increase
                from
                base.device_filmora_mac where   day= '__DAY1__' AND app_version is not null
                group by
                date_sub(next_day(last_active,'MO'),7),
                app_version
                ) week  order by week.stat_date desc
            ) t2
    """
    month_sql = """
        select 
        t1.* 
        from
        (   select
        month.stat_date,
        concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
       'filmora_win' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(last_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase
        from
       base.device_filmora_win where   day= '__DAY1__' AND app_version is not null
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
       'filmora_mac' as dev_type,
        month.app_version,
        month.increase
        from
        (
        select
        trunc(last_active, 'MM') as stat_date, -- 月的第一天
        app_version ,
        count(devid) as increase
        from
       base.device_filmora_mac where   day= '__DAY1__' AND app_version is not null
        group by
        trunc(last_active, 'MM'),  
        app_version
        ) month  order by month.stat_date desc
        
        ) t2
        
    """

    total_sql = """
        select
        t1.*
        from
        (
            select 
            stat_date,
            stat_date as show_date,
            total.dev_type,
            total.app_version,
            total.increase as total_amount
            from 
            (
            select 
            first_active as stat_date,
            app_version,
            'filmora_win' as dev_type,
            count(devid) as increase
            from base.device_filmora_win where app_version is not null and day='__DAY1__'
            GROUP BY first_active,app_version,'win'
            ) total
        ) t1
        union all
        select 
        t2.*
        from
        (
            select 
            stat_date,
            stat_date as show_date,
            total.dev_type,
            total.app_version,
            total.increase as total_amount
            from 
            (
            select 
            first_active as stat_date,
            app_version,
            'filmroa_mac' as dev_type,
            count(devid) as increase
            from base.device_filmora_win where app_version is not null and day='__DAY1__'
            GROUP BY first_active,app_version,'mac'
            ) total
        ) t2
    """
    if time_type == 'day':
        day_sql = job_day(date=excute_date,
                          moudle_sql=day_sql)
        insert_into_mysql(sql_query=day_sql, sql_session=sparksession,
                          table="device_active_day")

    if time_type == 'day':
        week_sql = job_day(date=excute_date,
                           moudle_sql=week_sql)
        insert_into_mysql(sql_query=week_sql, sql_session=sparksession,
                          table="device_active_week")

    if time_type == 'day':
        month_sql = job_day(date=excute_date,
                            moudle_sql=month_sql)
        insert_into_mysql(sql_query=month_sql, sql_session=sparksession,
                          table="device_active_month")

    if time_type == 'day':
        total_sql = job_day(date=excute_date,
                            moudle_sql=total_sql)
        print("begin to insert table device_total_day")
        insert_into_mysql(sql_query=total_sql, sql_session=sparksession,
                          table="device_total_day")
    sparksession.stop()

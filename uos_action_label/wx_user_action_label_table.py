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
/usr/local/bigdata/jobtaskh0/pythonjob/uos/uid_label/uos_action_label_day.py \
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
        .option("dbtable", "wx_user_action_label_table") \
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

    sparksession = (SparkSession
                    .builder
                    .appName('wx_user_action_label_table')
                    .enableHiveSupport()
                    .getOrCreate())

    # spark-submit 提供时间类型参数，计算日，周，月 day,week,month
    parser = argparse.ArgumentParser()
    parser.add_argument('time_type')
    parser.add_argument('excute_day')  # add_argument()指定程序可以接受的命令行选项
    args = parser.parse_args()
    time_type = args.time_type
    excute_date = parse(str(args.excute_day))



    tags_all_sql_sql="""
    
    insert overwrite table uos_com.tags_all partition(day= '__DAY1__') 
    select 
    t00.wsid,
    if(t11.export_count is null,0,t11.export_count) as export_count,
    t00.first_active as first_active,
    t00.last_active as last_active,
    if(t22.last_1m_duration is null,0,t22.last_1m_duration) as last_1m_duration,
    if(t22.last_1m_use_counts is null,0,t22.last_1m_use_counts) as last_1m_use_counts ,
    if(t44.last_3m_use_counts is null,0,t44.last_3m_use_counts) as last_3m_use_counts,
    case when t11.export_count>0 then 1 else 0  end as ever_rundown,
    case when datediff(current_date(),t00.last_active)>0 then 1 else 0 end as last_1m_ever_use ,
    if(t33.ever_export_youtube_vimeo is null,0,t33.ever_export_youtube_vimeo) as ever_export_youtube_vimeo,
    if(t66.ever_use_video_pannel is null ,0 ,t66.ever_use_video_pannel) as ever_use_video_pannel,              
    if(t66.ever_use_audio_pannel is null,0, t66.ever_use_audio_pannel) as ever_use_audio_pannel,                
    if(t66.ever_use_image_pannel is null,0, t66.ever_use_image_pannel) as ever_use_image_pannel,                
    if(t66.ever_use_effect_pannel is null,0,t66.ever_use_effect_pannel) as ever_use_effect_pannel,               
    if(t66.ever_use_transition_pannel is null,0,t66.ever_use_transition_pannel) as ever_use_transition_pannel,           
    if(t66.ever_use_text_pannel is null,0,t66.ever_use_text_pannel) as ever_use_text_pannel,                 
    if(t66.ever_use_advance_text_pannel is null,0,t66.ever_use_advance_text_pannel) as ever_use_advance_text_pannel,         
    if(t66.ever_use_crop_pannel is null,0,t66.ever_use_crop_pannel) as ever_use_crop_pannel,                 
    if(t66.ever_use_advanced_color_tuning_pannel is null,0,t66.ever_use_advanced_color_tuning_pannel) as ever_use_advanced_color_tuning_pannel,
    if(t55.ever_effect_usage_audio is null,0,t55.ever_effect_usage_audio) as ever_effect_usage_audio,              
    if(t55.ever_effect_usage_title is null,0,t55.ever_effect_usage_title) as ever_effect_usage_title,              
    if(t55.ever_effect_usage_transition is null,0,t55.ever_effect_usage_transition) as ever_effect_usage_transition,         
    if(t55.ever_effect_usage_effect is null,0,t55.ever_effect_usage_effect) as ever_effect_usage_effect,             
    if(t55.ever_effect_usage_element is null,0,t55.ever_effect_usage_element) as ever_effect_usage_element    
    from
     -- base.member
    (select t0.wsid,t0.first_active,t0.last_active 
    from (SELECT wsid,first_active,last_active , Row_Number() OVER (partition by wsid ORDER BY last_active desc)  rank from  base.member where day='__DAY1__' and wsid is not null) t0 where t0.rank=1
    ) t00
    left join 
    --  uos_com.total_export_times no_data
    (select t1.wsid,t1.export_count
    from (SELECT wsid,export_count , Row_Number() OVER (partition by wsid ORDER BY export_count desc)  rank from  uos_com.total_export_times where day='__DAY1__' ) t1 where t1.rank=1
    ) t11
    on t00.wsid=t11.wsid
    left join
    --  uos_com.dev_usage_month null data
    (
    select t2.wsid,t2.last_1m_duration,t2.last_1m_use_counts
    from (SELECT wsid,last_1m_duration,last_1m_use_counts , Row_Number() OVER (partition by wsid ORDER BY last_1m_duration desc)  rank from  uos_com.dev_usage_month where day=trunc(add_months('__DAY1__',-1),'MM') ) t2 where t2.rank=1
    ) t22
    on t00.wsid=t22.wsid
    left join 
    --uos_com.youtube_vimeo_label
    (
    select t3.wsid,t3.ever_export_youtube_vimeo
    from (SELECT wsid,ever_export_youtube_vimeo, Row_Number() OVER (partition by wsid ORDER BY ever_export_youtube_vimeo desc)  rank from  uos_com.youtube_vimeo_label where day='__DAY1__' ) t3 where t3.rank=1
    ) t33
    on t00.wsid=t33.wsid
    left join 
    -- uos_com.dev_quarter_usage_month null data 
    (
    select t4.wsid,case when t4.last_3m_use_counts is null then 0 else t4.last_3m_use_counts  end as last_3m_use_counts
    from (SELECT wsid,last_3m_use_counts, Row_Number() OVER (partition by wsid ORDER BY last_3m_use_counts desc)  rank from  uos_com.dev_quarter_usage_month where day= trunc(add_months('__DAY1__',-1),'MM') ) t4 where t4.rank=1
    )t44
    on t00.wsid=t44.wsid
    -- uos_com.effect_usage
    left join 
    (
    select 
    t5.wsid,
    t5.ever_effect_usage_audio,
    t5.ever_effect_usage_title,
    t5.ever_effect_usage_transition,
    t5.ever_effect_usage_effect,
    t5.ever_effect_usage_element
    from (SELECT wsid,ever_effect_usage_audio,ever_effect_usage_title,ever_effect_usage_transition,ever_effect_usage_effect,ever_effect_usage_element ,Row_Number() OVER (partition by wsid ORDER BY wsid desc)  rank from  uos_com.effect_usage where day='__DAY1__' ) t5 where t5.rank=1
    )t55 
    on t00.wsid=t55.wsid
    left join 
    --uos_com.screen
    (
    select 
    t6.wsid,
    t6.ever_use_video_pannel,
    t6.ever_use_audio_pannel,
    t6.ever_use_image_pannel,
    t6.ever_use_effect_pannel,
    t6.ever_use_transition_pannel,
    t6.ever_use_text_pannel,
    t6.ever_use_advance_text_pannel,
    t6.ever_use_crop_pannel,
    t6.ever_use_advanced_color_tuning_pannel
    from (SELECT wsid,ever_use_video_pannel,ever_use_audio_pannel,ever_use_image_pannel,ever_use_effect_pannel,ever_use_transition_pannel ,ever_use_text_pannel,ever_use_advance_text_pannel,ever_use_crop_pannel,ever_use_advanced_color_tuning_pannel,Row_Number() OVER (partition by wsid ORDER BY wsid desc)  rank from  uos_com.screen where day='__DAY1__' ) t6 where t6.rank=1
    ) t66
     on t00.wsid=t66.wsid
 
    
    """

    # 插入数据到uos_com.tags_all的最新分区
    if time_type == 'day':
        tags_all_sql_sql = job_day(date=excute_date,
                                           moudle_sql=tags_all_sql_sql)
        print("tags_all_sql_sql:", tags_all_sql_sql)
        excute(sql_query=tags_all_sql_sql, sql_session=sparksession)

    print("insert to uos_com.tags_all done!!")



    # 从uos_com.tags_all的最新分区读取数据，插入到mysql
    wx_user_action_label_sql = """
       select * from uos_com.tags_all  where day= '__DAY1__'

        """

    if time_type == 'day':
        wx_user_action_label_sql = job_day(date=excute_date,
                                           moudle_sql=wx_user_action_label_sql)
        insert_into_mysql(sql_query=wx_user_action_label_sql, sql_session=sparksession)

    sparksession.stop()

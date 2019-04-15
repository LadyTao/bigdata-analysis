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
                    .appName('uos_action_label_day')
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

    effect_usage_sql = """
    insert overwrite table uos_com.effect_usage partition(day= '__DAY1__') 
    select 
    t1.wsid,
    max(t1.ever_effect_usage_audio) as ever_effect_usage_audio,
    max(t1.ever_effect_usage_title) as ever_effect_usage_title,
    max(t1.ever_effect_usage_transition) as ever_effect_usage_transition,
    max(t1.ever_effect_usage_effect) as ever_effect_usage_effect,
    max(t1.ever_effect_usage_element) as ever_effect_usage_element
    from
    (
    select   wsid ,
    case when params["audio_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_audio,
    case when params["title_name_select"] !='null' then 1 else 0 end as ever_effect_usage_title,
    case when params["transition_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_transition,
    case when params["effect_name_select"] !='null' then 1 else 0 end as ever_effect_usage_effect,
    case when params["element_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_element
    from log.cl_filmora_win where day between  '__DAY1__' and '__DAY2__'  
     and wsid is not null and logtype="export" and logitem="effect_usage"
    
    ) t1
    group by t1.wsid
    UNION ALL
    select 
    t2.wsid,
    max(t2.ever_effect_usage_audio) as ever_effect_usage_audio,
    max(t2.ever_effect_usage_title) as ever_effect_usage_title,
    max(t2.ever_effect_usage_transition) as ever_effect_usage_transition,
    max(t2.ever_effect_usage_effect) as ever_effect_usage_effect,
    max(t2.ever_effect_usage_element) as ever_effect_usage_element
    from
    (
    select   wsid ,
    case when params["audio_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_audio,
    case when params["title_name_select"] !='null' then 1 else 0 end as ever_effect_usage_title,
    case when params["transition_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_transition,
    case when params["effect_name_select"] !='null' then 1 else 0 end as ever_effect_usage_effect,
    case when params["element_name_select"] !='null'  then 1 else 0 end as ever_effect_usage_element
    from log.cl_filmora_mac where day between  '__DAY1__' and '__DAY2__'
     and wsid is not null and logtype="export" and logitem="effect_usage"
    ) t2
    group by t2.wsid
    
    """

    screen_sql = """
    insert overwrite table uos_com.screen partition(day= '__DAY1__') 
    select 
    t1.wsid,
    max(t1.ever_use_video_pannel) as ever_use_video_pannel,
    max(t1.ever_use_audio_pannel) as ever_use_audio_pannel,
    max(t1.ever_use_image_pannel) as ever_use_image_pannel,
    max(t1.ever_use_effect_pannel) as ever_use_effect_pannel,
    max(t1.ever_use_transition_pannel) as ever_use_transition_pannel,
    max(t1.ever_use_text_pannel) as ever_use_text_pannel,
    max(t1.ever_use_advance_text_pannel) as ever_use_advance_text_pannel,
    max(t1.ever_use_crop_pannel) as ever_use_crop_pannel,
    max(t1.ever_use_advanced_color_tuning_pannel) as ever_use_advanced_color_tuning_pannel
    from 
    (
    select   wsid ,
    case when logitem='video_property'   then 1 else 0 end as ever_use_video_pannel,
    case when logitem='audio_property' then 1 else 0 end as ever_use_audio_pannel,
    case when logitem='image_property'  then 1 else 0 end as ever_use_image_pannel,
    case when logitem='effect_property'  then 1 else 0 end as ever_use_effect_pannel,
    case when logitem='transition_property'   then 1 else 0 end as ever_use_transition_pannel,
    case when logitem='text_property'  then 1 else 0 end as ever_use_text_pannel,
    case when logitem='advanced_text_edit'   then 1 else 0 end as ever_use_advance_text_pannel,
    case when logitem='crop' then 1 else 0 end as ever_use_crop_pannel,
    case when logitem='advanced_color_tuning'  then 1 else 0 end as ever_use_advanced_color_tuning_pannel
    from log.cl_filmora_win where  day between  '__DAY1__' and '__DAY2__'
     and wsid is not null and logtype="screen" and logitem in ("video_property","audio_property","image_property","effect_property","transition_property","text_property","advanced_text_edit","crop","advanced_color_tuning")
    
    ) t1
    group by t1.wsid 
    union all 
    select 
    t2.wsid,
    max(t2.ever_use_video_pannel) as ever_use_video_pannel,
    max(t2.ever_use_audio_pannel) as ever_use_audio_pannel,
    max(t2.ever_use_image_pannel) as ever_use_image_pannel,
    max(t2.ever_use_effect_pannel) as ever_use_effect_pannel,
    max(t2.ever_use_transition_pannel) as ever_use_transition_pannel,
    max(t2.ever_use_text_pannel) as ever_use_text_pannel,
    max(t2.ever_use_advance_text_pannel) as ever_use_advance_text_pannel,
    max(t2.ever_use_crop_pannel) as ever_use_crop_pannel,
    max(t2.ever_use_advanced_color_tuning_pannel) as ever_use_advanced_color_tuning_pannel
    from 
    (
    select   wsid ,
    case when logitem='video_property'   then 1 else 0 end as ever_use_video_pannel,
    case when logitem='audio_property' then 1 else 0 end as ever_use_audio_pannel,
    case when logitem='image_property'  then 1 else 0 end as ever_use_image_pannel,
    case when logitem='effect_property'  then 1 else 0 end as ever_use_effect_pannel,
    case when logitem='transition_property'   then 1 else 0 end as ever_use_transition_pannel,
    case when logitem='text_property'  then 1 else 0 end as ever_use_text_pannel,
    case when logitem='advanced_text_edit'   then 1 else 0 end as ever_use_advance_text_pannel,
    case when logitem='crop' then 1 else 0 end as ever_use_crop_pannel,
    case when logitem='advanced_color_tuning'  then 1 else 0 end as ever_use_advanced_color_tuning_pannel
    from log.cl_filmora_mac where day between  '__DAY1__' and '__DAY2__' 
     and wsid is not null and logtype="screen" and logitem in ("video_property","audio_property","image_property","effect_property","transition_property","text_property","advanced_text_edit","crop","advanced_color_tuning")
    ) t2
    group by t2.wsid
    
    """


    total_export_times_sql= """
        insert overwrite table uos_com.total_export_times partition(day='__DAY1__') 
        select wsid, count(wsid)  export_count from 
        (select wsid from log.cl_filmora_win where day between  '__DAY1__' and '__DAY2__'  and wsid is not null and logtype='export' and logitem='export_success'
        union all
        select wsid  from log.cl_filmora_mac where day between  '__DAY1__' and '__DAY2__'   and wsid is not null and logtype='export' and logitem='export_success' 
        ) n group by wsid 
    
     
        """
    youtube_vimeo_lable_sql = """
    insert overwrite table uos_com.youtube_vimeo_label partition(day= '__DAY1__') 
        select 
        t1.wsid,
        max(t1.ever_export_youtube_vimeo) as ever_export_youtube_vimeo
        from
        (
        select   wsid ,
        case when upper(params["export_type"]) IN ("YOUTUBE","VIMEO")  then 1 else 0 end as ever_export_youtube_vimeo
        from log.cl_filmora_win where day between  '__DAY1__' and '__DAY2__'  
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
        from log.cl_filmora_mac where day between  '__DAY1__' and '__DAY2__'
        and wsid is not null and logtype="export" and logitem="export_format" and params['export_type'] in ("YouTube","Vimeo") 
        
        ) t2
        group by t2.wsid

    
    """


    member_sql ="""
        insert overwrite table base.member partition(day= '__DAY1__') 
        select wsid,min(day) as first_active,max(day) last_active  from 
        (
        select wsid,day from log.cl_filmora_win where day < '__DAY1__'
        and logtype= 'base' and logitem='login_status' and wsid is not null
        union all
        select wsid,day from log.cl_filmora_mac where day < '__DAY1__'
        and logtype= 'base' and logitem='login_status'  and wsid is not null ) 
        m group by wsid  
        
        """

    if time_type == 'day':
        effect_usage_sql = job_day(date=excute_date,
                                   moudle_sql=effect_usage_sql)
        print("effect_usage_sql:", effect_usage_sql)
        print("begin to insert uos_com.effect_usage table ")
        excute(sql_query=effect_usage_sql, sql_session=sparksession)

        screen_sql = job_day(date=excute_date,
                                   moudle_sql=screen_sql)
        print("screen_sql:", screen_sql)
        print("begin to insert uos_com.screen table ")
        excute(sql_query=screen_sql, sql_session=sparksession)

        total_export_times_sql = job_day(date=excute_date,
                             moudle_sql=total_export_times_sql)
        print("total_export_times_sql:", total_export_times_sql)
        print("begin to insert uos_com.total_export_times table ")
        excute(sql_query=total_export_times_sql, sql_session=sparksession)

        youtube_vimeo_lable_sql = job_day(date=excute_date,
                                         moudle_sql=youtube_vimeo_lable_sql)
        print("youtube_vimeo_lable_sql:", youtube_vimeo_lable_sql)
        print("begin to insert uos_com.youtube_vimeo_lable table ")
        excute(sql_query=youtube_vimeo_lable_sql, sql_session=sparksession)


        member_sql = job_day(date=excute_date,
                                          moudle_sql=member_sql)
        print("member_sql:", member_sql)
        print("begin to insert base.member table ")
        excute(sql_query=member_sql, sql_session=sparksession)

    sparksession.stop()

# -*- coding: UTF-8 -*-

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pprintpp import pprint as pp
import logging
import pyspark.sql.functions as F

import datetime
import calendar
import argparse
from dateutil.parser import parse
logging.getLogger("org").setLevel("ERROR")
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
/usr/local/bigdata/jobtaskh0/pythonjob/uos/client_base_info/cart_conver_funnel.py \
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


def insert_to_hive(sql_query, sql_session):
    """
    此处可以insert语句直接写到相关sql中，执行完sql ,直接将相关的数据插入到hive中的相表。
    :params sql_query: insert cluase
    :params sql_session:  SparkSession
    """
    print("%%%%%%%%%%%%%%%%% begin to insert data to hive%%%%%%%%%%%%%%%%%%%%%")
    df = sql_session.sql(sql_query)
    print("%%%%%%%%%%%%%%%%%%%%%%insert into hive table done%%%%%%%%%%%%%%%%%%")


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
    print("insert to mysql  table: " + table + "   succeed!!!")


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
                    .appName('device_filmora')
                    .enableHiveSupport()
                    .getOrCreate())

    # spark-submit 提供时间类型参数，计算日，周，月 day,week,month
    parser = argparse.ArgumentParser()
    parser.add_argument('time_type')
    parser.add_argument('excute_day')  # add_argument()指定程序可以接受的命令行选项
    args = parser.parse_args()
    time_type = args.time_type
    excute_date = parse(str(args.excute_day))

    sql_day_filmstocks = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
    select
    "__DAY1__" as show_date,
    "filmstocks" as produce,
    "day" as date_type,
    tb0.site_type,
    plan_num, plan_user_num, checkout_num, checkout_user_num,
    generate_num, succeed_num, falied_num, unpay_num,
    round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
    round(succeed_num / generate_num, 4) as pay_convert_rate 
from
    (SELECT         
        site_type,
        count(if(url like "%plan.html", wsid, null)) as plan_num,
        count(distinct if(url like "%plan.html", wsid, null)) as plan_user_num,
        count(if(url like "%/pay/checkout.html%", wsid, null)) as checkout_num,
        count(distinct if(url like "%/pay/checkout.html%", wsid, null)) as checkout_user_num
    from
        (select             
            case when cid=10701 then "Filmstocks英语站"
            when cid=10702 then "Filmstocks法语站"
            when cid=10703 then "Filmstocks葡萄牙语站"
            when cid=10704 then "Filmstocks西班牙语站"
            when cid=10705 then "Filmstocks德语站"
            when cid=10706 then "Filmstocks日语站"
            when cid=10707 then "Filmstocks意大利语站" end as site_type,
            url,
            wsid
            from log.wl_filmstocks_com
        where day="__DAY1__" and cid in (10701, 10702, 10703, 10704, 10705, 10706, 10707) and (url like "%plan.html" or url like "%/pay/checkout.html%"))x0
    group by site_type
    ) tb0
JOIN
    (select         
        site_type,
        count(*) as generate_num,
        count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
        count(distinct if(status="40", order_no, null)) as falied_num,
        count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
    from (select        
        case when from_site="24" then "Filmstocks英语站"
        when from_site="25" then "Filmstocks法语站"
        when from_site="26" then "Filmstocks葡萄牙语站"
        when from_site="27" then "Filmstocks西班牙语站"
        when from_site="28" then "Filmstocks德语站"
        when from_site="29" then "Filmstocks日语站"
        when from_site="30" then "Filmstocks意大利语站" end as site_type,
        status, order_no
    from dbsync.f_order_wx_order 
    where origin="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd")="__DAY1__" and from_site in ("24","25","26","27","28","29","30")) x1
    group by site_type
    ) tb1
on tb0.site_type=tb1.site_type
'''

    sql_week_filmstocks = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
    select
        concat("__DAY1__","~","__DAY2__") as show_date,
        "filmstocks" as produce,
        "week" as date_type,
        tb0.site_type,
        plan_num, plan_user_num, checkout_num, checkout_user_num,
        generate_num, succeed_num, falied_num, unpay_num,
        round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
        round(succeed_num / generate_num, 4) as pay_convert_rate        
    from
        (SELECT             
            site_type,
            count(if(url like "%plan.html", wsid, null)) as plan_num,
            count(distinct if(url like "%plan.html", wsid, null)) as plan_user_num,
            count(if(url like "%/pay/checkout.html%", wsid, null)) as checkout_num,
            count(distinct if(url like "%/pay/checkout.html%", wsid, null)) as checkout_user_num
        from
            (select                
                case when cid=10701 then "Filmstocks英语站"
                when cid=10702 then "Filmstocks法语站"
                when cid=10703 then "Filmstocks葡萄牙语站"
                when cid=10704 then "Filmstocks西班牙语站"
                when cid=10705 then "Filmstocks德语站"
                when cid=10706 then "Filmstocks日语站"
                when cid=10707 then "Filmstocks意大利语站" end as site_type,
                url,
                wsid
            from log.wl_filmstocks_com
            where day between "__DAY1__" and "__DAY2__" and cid in (10701, 10702, 10703, 10704, 10705, 10706, 10707) and (url like "%plan.html" or url like "%/pay/checkout.html%"))x0
        group by site_type
        ) tb0
    JOIN
        (select            
            site_type,
            count(*) as generate_num,
            count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
            count(distinct if(status="40", order_no, null)) as falied_num,
            count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
        from(select            
            case when from_site="24" then "Filmstocks英语站"
            when from_site="25" then "Filmstocks法语站"
            when from_site="26" then "Filmstocks葡萄牙语站"
            when from_site="27" then "Filmstocks西班牙语站"
            when from_site="28" then "Filmstocks德语站"
            when from_site="29" then "Filmstocks日语站"
            when from_site="30" then "Filmstocks意大利语站" end as site_type,
            status, order_no
        from dbsync.f_order_wx_order
        where origin="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd") between "__DAY1__" and "__DAY2__" and from_site in ("24","25","26","27","28","29","30")) x1
        group by site_type
        ) tb1
    on tb0.site_type=tb1.site_type'''

    sql_month_filmstocks = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
    select
        concat("__DAY1__", "~", "__DAY2__") as show_date, 
        "filmstocks" as produce,
        "month" as date_type,
        tb0.site_type,
        plan_num, plan_user_num, checkout_num, checkout_user_num,
        generate_num, succeed_num, falied_num, unpay_num,
        round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
        round(succeed_num / generate_num, 4) as pay_convert_rate        
    from
        (SELECT
            "__DAY1__" as day,
            site_type,
            count(if(url like "%plan.html", wsid, null)) as plan_num,
            count(distinct if(url like "%plan.html", wsid, null)) as plan_user_num,
            count(if(url like "%/pay/checkout.html%", wsid, null)) as checkout_num,
            count(distinct if(url like "%/pay/checkout.html%", wsid, null)) as checkout_user_num
        from
            (select
                case when cid=10701 then "Filmstocks英语站"
                when cid=10702 then "Filmstocks法语站"
                when cid=10703 then "Filmstocks葡萄牙语站"
                when cid=10704 then "Filmstocks西班牙语站"
                when cid=10705 then "Filmstocks德语站"
                when cid=10706 then "Filmstocks日语站"
                when cid=10707 then "Filmstocks意大利语站" end as site_type,
                url,
                wsid
            from log.wl_filmstocks_com
        where day between "__DAY1__" and "__DAY2__" and cid in (10701, 10702, 10703, 10704, 10705, 10706, 10707) and (url like "%plan.html" or url like "%/pay/checkout.html%"))x0
        group by site_type
        ) tb0
    JOIN
        (select
            site_type,
            count(*) as generate_num,
            count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
            count(distinct if(status="40", order_no, null)) as falied_num,
            count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
        from(select
            case when from_site="24" then "Filmstocks英语站"
            when from_site="25" then "Filmstocks法语站"
            when from_site="26" then "Filmstocks葡萄牙语站"
            when from_site="27" then "Filmstocks西班牙语站"
            when from_site="28" then "Filmstocks德语站"
            when from_site="29" then "Filmstocks日语站"
            when from_site="30" then "Filmstocks意大利语站" end as site_type,
            status, order_no
        from dbsync.f_order_wx_order
        where origin="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd") between "__DAY1__" and "__DAY2__" and from_site in ("24","25","26","27","28","29","30")) x1
        group by site_type
        ) tb1
    on tb0.site_type=tb1.site_type'''

    sql_day_filmora = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
    select 
    "__DAY1__" as show_date,
    "filmora" as produce,
    "day" as date_type,
    tb0.site_type,
    plan_num, plan_user_num, checkout_num, checkout_user_num,
    generate_num, succeed_num, falied_num, unpay_num,
    round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
    round(succeed_num / generate_num, 4) as pay_convert_rate 
    from
    (select
        site_type,
        count(if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_num,
        count(distinct if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_user_num,
        count(if(url like "%checkout.html%", wsid, null)) as checkout_num,
        count(distinct if(url like "%checkout.html%", wsid, null)) as checkout_user_num
    from 
        (SELECT
            url, wsid,
            case when url like "%wondershare.com/fr%" then "Wondershare法语" 
            when url like "%wondershare.com/jp%" then "Wondershare日语" 
            when url like "%wondershare.com/de%" then "Wondershare德语" 
            when url like "%wondershare.com/es%" then "Wondershare西班牙语" 
            when url like "%wondershare.com/pt%" then "Wondershare葡萄牙语" 
            when url like "%wondershare.com/it%" then "Wondershare意大利语" 
            when url like "%wondershare.com/net%" then "WondershareCPC英语" 
            when url like "%wondershare.com%" then "wondershare英文站" 
            else "others" end as site_type
        from
            (select url, wsid from log.wl_wondershare_com where day="__DAY1__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%") 
            union all
            select url, wsid from log.wl_wondershare_jp where day="__DAY1__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            union all
            select url, wsid from log.wl_wondershare_net where day="__DAY1__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            ) x0 
        )m0
    group by site_type)tb0
    join
    (select
        site_type,
        count(*) as generate_num,
        count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
        count(distinct if(status="40", order_no, null)) as falied_num,
        count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
    from (select
            case when from_site="1" then "Wondershare英文站"
            when from_site="2" then "Wondershare法语"
            when from_site="3" then "Wondershare葡萄牙语"
            when from_site="4" then "Wondershare西班牙语"
            when from_site="5" then "Wondershare德语"
            when from_site="6" then "Wondershare日语"
            when from_site="7" then "Wondershare意大利语" 
            when from_site="8" then "WondershareCPC英语" 
            else "others" end as site_type,
            status, order_no
        from dbsync.f_order_wx_order 
        where origin!="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd")="__DAY1__" and from_site in ("1","2","3","4","5","6","7","8")) x1
    group by site_type) tb1
    on tb0.site_type=tb1.site_type'''

    sql_week_filmora = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
        select 
    concat("__DAY1__", "~", "__DAY2__") as show_date,
    "filmora" as produce,
    "week" as date_type,
    tb0.site_type,
    plan_num, plan_user_num, checkout_num, checkout_user_num,
    generate_num, succeed_num, falied_num, unpay_num,
    round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
    round(succeed_num / generate_num, 4) as pay_convert_rate
    from
    (select
        site_type,
        count(if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_num,
        count(distinct if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_user_num,
        count(if(url like "%checkout.html%", wsid, null)) as checkout_num,
        count(distinct if(url like "%checkout.html%", wsid, null)) as checkout_user_num
    from 
        (SELECT
            url, wsid,
            case when url like "%wondershare.com/fr%" then "Wondershare法语" 
            when url like "%wondershare.com/jp%" then "Wondershare日语" 
            when url like "%wondershare.com/de%" then "Wondershare德语" 
            when url like "%wondershare.com/es%" then "Wondershare西班牙语" 
            when url like "%wondershare.com/pt%" then "Wondershare葡萄牙语" 
            when url like "%wondershare.com/it%" then "Wondershare意大利语" 
            when url like "%wondershare.com/net%" then "WondershareCPC英语" 
            when url like "%wondershare.com%" then "wondershare英文站" 
            else "others" end as site_type
        from
            (select url, wsid from log.wl_wondershare_com where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%") 
            union all
            select url, wsid from log.wl_wondershare_jp where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            union all
            select url, wsid from log.wl_wondershare_net where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            ) x0 
        )m0
    group by site_type)tb0
    join
    (select
        site_type,
        count(*) as generate_num,
        count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
        count(distinct if(status="40", order_no, null)) as falied_num,
        count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
    from (select
            case when from_site="1" then "Wondershare英文站"
            when from_site="2" then "Wondershare法语"
            when from_site="3" then "Wondershare葡萄牙语"
            when from_site="4" then "Wondershare西班牙语"
            when from_site="5" then "Wondershare德语"
            when from_site="6" then "Wondershare日语"
            when from_site="7" then "Wondershare意大利语" 
            when from_site="8" then "WondershareCPC英语" 
            else "others" end as site_type,
            status, order_no
        from dbsync.f_order_wx_order 
        where origin!="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd") between "__DAY1__" and "__DAY2__" and from_site in ("1","2","3","4","5","6","7","8")) x1
    group by site_type) tb1
on tb0.site_type=tb1.site_type'''

    sql_month_filmora = '''insert into table `tmp`.`shopping_cart_funnel` partition(stat_date="__DAY1__")
            select 
    concat("__DAY1__", "~", "__DAY2__") as show_date,
    "filmora" as produce,
    "month" as date_type,
    tb0.site_type,
    plan_num, plan_user_num, checkout_num, checkout_user_num,
    generate_num, succeed_num, falied_num, unpay_num,
    round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
    round(succeed_num / generate_num, 4) as pay_convert_rate 
    from
    (select
        site_type,
        count(if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_num,
        count(distinct if(url like "%filmora-buy.html%" or url like "%buy-video-editor.html%", wsid, null)) as plan_user_num,
        count(if(url like "%checkout.html%", wsid, null)) as checkout_num,
        count(distinct if(url like "%checkout.html%", wsid, null)) as checkout_user_num
    from 
        (SELECT
            url, wsid,
            case when url like "%wondershare.com/fr%" then "Wondershare法语" 
            when url like "%wondershare.com/jp%" then "Wondershare日语" 
            when url like "%wondershare.com/de%" then "Wondershare德语" 
            when url like "%wondershare.com/es%" then "Wondershare西班牙语" 
            when url like "%wondershare.com/pt%" then "Wondershare葡萄牙语" 
            when url like "%wondershare.com/it%" then "Wondershare意大利语" 
            when url like "%wondershare.com/net%" then "WondershareCPC英语" 
            when url like "%wondershare.com%" then "wondershare英文站" 
            else "others" end as site_type
        from
            (select url, wsid from log.wl_wondershare_com where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%") 
            union all
            select url, wsid from log.wl_wondershare_jp where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            union all
            select url, wsid from log.wl_wondershare_net where day between "__DAY1__" and "__DAY2__" and (url like "%filmora-buy.html%" or url like "%buy-video-editor.html%" or url like "%checkout.html%")
            ) x0 
        )m0
    group by site_type)tb0
    join
    (select
        site_type,
        count(*) as generate_num,
        count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
        count(distinct if(status="40", order_no, null)) as falied_num,
        count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
    from (select
            case when from_site="1" then "Wondershare英文站"
            when from_site="2" then "Wondershare法语"
            when from_site="3" then "Wondershare葡萄牙语"
            when from_site="4" then "Wondershare西班牙语"
            when from_site="5" then "Wondershare德语"
            when from_site="6" then "Wondershare日语"
            when from_site="7" then "Wondershare意大利语" 
            when from_site="8" then "WondershareCPC英语" 
            else "others" end as site_type,
            status, order_no
        from dbsync.f_order_wx_order 
        where origin!="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd") between "__DAY1__" and "__DAY2__" and from_site in ("1","2","3","4","5","6","7","8")) x1
    group by site_type) tb1
    on tb0.site_type=tb1.site_type'''

    if time_type == 'day':
        sql_stocks = job_day(date=excute_date, moudle_sql=sql_day_filmstocks)
        sql_filmora = job_day(date=excute_date, moudle_sql=sql_day_filmora)

        print("begin to insert tmp.shopping_cart_funnel table%")
        insert_to_hive(sql_query=sql_stocks, sql_session=sparksession)
        insert_to_hive(sql_query=sql_filmora, sql_session=sparksession)

        sqlx = "select stat_date, show_date, produce, site_type, plan_num," \
               " plan_user_num,checkout_num,checkout_user_num, generate_num," \
               "succeed_num,falied_num, unpay_num " \
               "from tmp.shopping_cart_funnel " \
               "where stat_date='__DAY1__' and date_type='day'"

        sqly = "select stat_date, show_date, produce, site_type, " \
               "pay_succeed_rate, pay_convert_rate from " \
               "tmp.shopping_cart_funnel " \
               "where stat_date='__DAY1__' and date_type='day'"

        insert_into_mysql(sql_query=job_day(date=excute_date, moudle_sql=sqlx),
                          sql_session=sparksession,
                          table="funnel_shopcart_number_day")

        insert_into_mysql(sql_query=job_day(date=excute_date, moudle_sql=sqly),
                          sql_session=sparksession,
                          table="funnel_shopcart_rate_day")

    if time_type == 'week':
        sql_stocks = job_week(date=excute_date, moudle_sql=sql_week_filmstocks)
        sql_filmora = job_week(date=excute_date, moudle_sql=sql_week_filmora)
        print("begin to insert tmp.shopping_cart_funnel table ")
        insert_to_hive(sql_query=sql_stocks, sql_session=sparksession)
        insert_to_hive(sql_query=sql_filmora, sql_session=sparksession)

        sqlx = "select stat_date, show_date, produce, site_type, plan_num, " \
               "plan_user_num, checkout_num, checkout_user_num, generate_num, " \
               "succeed_num,falied_num, unpay_num from tmp.shopping_cart_funnel" \
               " where stat_date='__DAY1__' and date_type='week'"

        sqly = "select stat_date, show_date, produce, site_type, " \
               "pay_succeed_rate,pay_convert_rate " \
               "from tmp.shopping_cart_funnel " \
               "where stat_date='__DAY1__' and date_type='week'"
        insert_into_mysql(sql_query=job_week(date=excute_date, moudle_sql=sqlx),
                          sql_session=sparksession,
                          table="funnel_shopcart_number_week")

        insert_into_mysql(sql_query=job_week(date=excute_date, moudle_sql=sqly),
                          sql_session=sparksession,
                          table="funnel_shopcart_rate_week")

    if time_type == 'month':
        sql_stocks = job_month(date=excute_date, moudle_sql=sql_month_filmstocks)
        sql_filmora = job_month(date=excute_date, moudle_sql=sql_month_filmora)
        print("begin to insert tmp.shopping_cart_funnel table ")
        insert_to_hive(sql_query=sql_stocks, sql_session=sparksession)
        insert_to_hive(sql_query=sql_filmora, sql_session=sparksession)

        sqlx = "select stat_date, show_date, produce, site_type, plan_num, " \
               "plan_user_num, checkout_num, checkout_user_num, generate_num, " \
               "succeed_num,falied_num,unpay_num from tmp.shopping_cart_funnel " \
               "where stat_date='__DAY1__' and date_type='month'"

        sqly = "select stat_date, show_date, produce, site_type, " \
               "pay_succeed_rate, pay_convert_rate " \
               "from tmp.shopping_cart_funnel " \
               "where stat_date='__DAY1__' and date_type='month'"
        insert_into_mysql(sql_query=job_month(date=excute_date, moudle_sql=sqlx),
                          sql_session=sparksession,
                          table="funnel_shopcart_number_month")

        insert_into_mysql(sql_query=job_month(date=excute_date, moudle_sql=sqly),
                          sql_session=sparksession,
                          table="funnel_shopcart_rate_month")

    sparksession.stop()


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
    print("begin to insert data to hive")
    df = sql_session.sql(sql_query)
    print("insert into hive table done")


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

    sqlx = '''insert overwrite table tmp.shopping_cart_filmstocks_funnel partition(day=__DAY1__)
    select
	-- 维度
	"filmstocks" as produce,
	tb0.site_type,
	--基础数据
	plan_num, plan_user_num, checkout_num, checkout_user_num,
	generate_num, succeed_num, falied_num, unpay_num,
	--指标数据
	round(succeed_num / (succeed_num + falied_num), 4) as pay_succeed_rate,
	round(succeed_num / generate_num, 4) as pay_convert_rate,
	--预警机制 --> 加入购物车次数(PV/UV)，实际计算方法为：checkout的PV/UV
	round(checkout_num / checkout_user_num, 2) as add_to_shoppingcar
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
		where day=__DAY1__ and cid in (10701, 10702, 10703, 10704, 10705, 10706, 10707) and (url like "%plan.html" or url like "%/pay/checkout.html%"))x0
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
	where origin="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd")=__DAY1__ and from_site in ("24","25","26","27","28","29","30")) x1
	group by site_type
	) tb1
on tb0.site_type=tb1.site_type;
    '''

    device_filmora_win_sql = job_day(date=excute_date,
                                   moudle_sql=sqlx)
    print("device_filmora_win_sql:", device_filmora_win_sql)
    print("begin to insert tmp.shopping_cart_filmstocks_funnel table ")
    insert_to_hive(sql_query=device_filmora_win_sql, sql_session=sparksession)

    sparksession.stop()







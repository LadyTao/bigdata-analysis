# -*- coding: UTF-8 -*-

import sys
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pprintpp import pprint as pp

import pyspark.sql.functions as F

"""
spark-submit pyspark  file with special resources demo

./bin/spark-submit    --num-executors 4 --executor-memory 4G --executor-cores 4 --driver-memory 4G --driver-cores 4 path/somefile.py


 插入mysql 时一定要添加  --jars  --driver-class-path  
./bin/spark-submit  --num-executors 4 --executor-memory 4G --executor-cores 4 --driver-memory 4G --driver-cores 4 --jars /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar --driver-class-path /usr/hdp/3.0.1.0-187/spark2/jars/mysql-connector-java-5.1.47.jar  /usr/local/bigdata/jobtaskh0/pythonjob/uos/uid_label/test.py 



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


if __name__ == "__main__":
    SparkContext.setSystemProperty("hive.metastore.uris", "thrift://hdp-0:9083")

    sparksession = (SparkSession
                    .builder
                    .appName('select_insert_mysql_hive')
                    .enableHiveSupport()
                    .getOrCreate())

    sql_str = '''
				select * from uos_com.tags_all where day='2019-03-24'
			 '''

    excute(sql_query=sql_str, sql_session=sparksession)

    insert_into_mysql(sql_query=sql_str, sql_session=sparksession)

    # 插入hive分区表信息，此处需要提供相关的插入hive分区表语句
    insert_sql_str = """
    insert overwrite table uos_com.screen partition(day= '2019-03-26') 
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
    from log.cl_filmora_win where day='2019-03-26'  
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
    from log.cl_filmora_mac where day='2019-03-26'  
     and wsid is not null and logtype="screen" and logitem in ("video_property","audio_property","image_property","effect_property","transition_property","text_property","advanced_text_edit","crop","advanced_color_tuning")
    ) t2
    group by t2.wsid
    """
    insert_to_hive(sql_query=insert_sql_str, sql_session=sparksession)
    sparksession.stop()


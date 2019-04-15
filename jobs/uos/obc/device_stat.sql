
-- 终端信息基表
drop table uos_com.client_base_info;
CREATE EXTERNAL TABLE `client_base_info`(
   `devid` string,
   `version` string,
   `fisrt_active` string,
   `last_active` string,
   `platform` string
)
 PARTITIONED BY (
   `day` string)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
 LOCATION
   'hdfs://hdp-0.local:8020/warehouse/tablespace/external/hive/uos_com.db/client_base_info' ;



insert overwrite table uos_com.client_base_info partition(day='2019-04-03')
select
t1.devid,
max(t1.version) as version,
min(t1.day) as fisrt_active,
max(t1.day) as last_active,
max(t1.os_platform) as platform
from
(select
devid,
params['app_version'] as version,
'win' as os_platform,
day
from log.cl_filmora_win where logitem='app_launch' and devid is not null and day <'2019-04-03'
) t1
group by t1.devid
union  all
select
t2.devid,
max(t2.version) as version,
min(t2.day) as fisrt_active,
max(t2.day) as last_active,
max(t2.os_platform) as platform
from
(select
devid,
params['app_version'] as version,
'mac' as os_platform,
day
from log.cl_filmora_mac where logitem='app_launch' and devid is not null and day <'2019-04-03'
) t2
group by t2.devid



-- 累计终端
    -- 综合概况


--data_user.device_total_day

select
date_sub(current_date,1) as stat_date,
date_sub(current_date,1) as show_date,
total.dev_type,
total.app_version,
total.increase as total_amount
from
(
select
platform as dev_type,
version as app_version ,
count(devid) as increase
from
uos_com.client_base_info where   version is not null
group by
platform,
version
) total
 order by total.increase desc





-- 新增终端
    --维度 win，mac .
    --
    --版本：版本
    --
    --维度 日，周，月
    --
    --展现形式:曲线, 数据
-- 日新增

-- data_user.device_increase_day
use uos_com;

        select
            day.stat_date,
            day.stat_date as show_date,
            day.dev_type,
            day.app_version,
            day.increase
            from
            (
            select
            fisrt_active as stat_date,
            -- date_sub(next_day(date_sub(day,1),'MO'),7) as stat_week,
            -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
            platform as dev_type,
            version as app_version ,
            count(devid) as increase
            from
            uos_com.client_base_info where   version is not null
            group by
            fisrt_active,
            platform,
            version
            order by fisrt_active desc
            ) day  order by day.stat_date desc



-- 周新增
-- data_user.device_increase_week
use uos_com;

 select
        week.stat_date,
        concat(week.stat_date,'~',date_add(week.stat_date,6)) as show_date,
        week.dev_type,
        week.app_version,
        week.increase
        from
        (
        select
        -- fisrt_active as stat_date,
        date_sub(next_day(date_sub(fisrt_active,1),'MO'),7) as stat_date, -- 周的第一天
        -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
        platform as dev_type,
        version as app_version ,
        count(devid) as increase
        from
        uos_com.client_base_info where   version is not null
        group by
        date_sub(next_day(date_sub(fisrt_active,1),'MO'),7),
        platform,
        version
        ) week  order by week.stat_date desc


-- 月新增

use uos_com;
-- -- data_user.device_increase_month
select
    month.stat_date,
    concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
    month.dev_type,
    month.app_version,
    month.increase
    from
    (
    select
    trunc(fisrt_active, 'MM') as stat_date, -- 月的第一天
    platform as dev_type,
    version as app_version ,
    count(devid) as increase
    from
    uos_com.client_base_info where   version is not null
    group by
    trunc(fisrt_active, 'MM'),
    platform,
    version
    ) month  order by month.stat_date desc



-- 终端活跃
    --维度 win，mac .
    --
    --版本：版本
    --
    --维度 日，周，月
    --
    --展现形式:曲线, 数据

-- 日活跃
-- data_user.device_active_day

        select
            day.stat_date,
            day.stat_date as show_date,
            day.dev_type,
            day.app_version,
            day.increase
            from
            (
            select
            last_active as stat_date,
            -- date_sub(next_day(date_sub(day,1),'MO'),7) as stat_week,
            -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
            platform as dev_type,
            version as app_version ,
            count(devid) as increase
            from
            uos_com.client_base_info where   version is not null
            group by
            last_active,
            platform,
            version
            order by last_active desc
            ) day  order by day.stat_date desc

-- 周活跃
-- data_user.device_active_week
        select
        week.stat_date,
        concat(week.stat_date,'~',date_add(week.stat_date,6)) as show_date,
        week.dev_type,
        week.app_version,
        week.increase
        from
        (
        select
        -- fisrt_active as stat_date,
        date_sub(next_day(date_sub(last_active,1),'MO'),7) as stat_date, -- 周的第一天
        -- concat(substr(date_sub(day,1),0,7),'-01') as stat_month,
        platform as dev_type,
        version as app_version ,
        count(devid) as increase
        from
        uos_com.client_base_info where   version is not null
        group by
        date_sub(next_day(date_sub(last_active,1),'MO'),7),
        platform,
        version
        ) week  order by week.stat_date desc
-- 月活跃
-- data_user.device_active_month


   select
    month.stat_date,
    concat(month.stat_date,'~',date_sub(add_months(month.stat_date,1),1)) as show_date,
    month.dev_type,
    month.app_version,
    month.increase
    from
    (
    select
    trunc(last_active, 'MM') as stat_date, -- 月的第一天
    platform as dev_type,
    version as app_version ,
    count(devid) as increase
    from
    uos_com.client_base_info where   version is not null
    group by
    trunc(last_active, 'MM'),
    platform,
    version
    ) month  order by month.stat_date desc




-- 留存分析
-- 参考地址:https://mobile.umeng.com/platform/5b8cf21af43e481aea000022/reports/retention
    --维度 win，mac
    --
    --维度 日，周，月
    --
    --留存图
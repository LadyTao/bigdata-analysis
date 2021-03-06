

use dbsync;
insert overwrite  table mart.ci_member_renew_rate_day partition(expire_date='${hiveconf:expire}')
-- insert  into table mart.ci_member_renew_rate_day1
select
from_unixtime(t3.endtime+28800,'yyyy-MM-dd') as stat_date,
t3.channel,
expire_time_type,
expire_user_level,
renew_time_type,
renew_user_level,
count(t3.uid) as expire_user,
count(t4.uid) as renew_user
from
(
select
t1.uid,
t2.channel,
t1.vip_type as expire_user_level,
t1.endtime,
t1.pay_mode as expire_time_type
from
(select
uid,
-- vip_type,
case when vip_type='1' then '普通会员'
     when vip_type='2' then '高级会员'
     when vip_type='3' then 'VIP会员'
     when vip_type='4' then '企业会员'
else '其他' end as vip_type,
endtime,
--from_unixtime(endtime+28800,'yyyy-MM-dd') as endtime,
spread_origin,
-- pay_mode
case when pay_mode='month' then '月付'
     when pay_mode='year' then '年付'
else '其他'  end as pay_mode
from ci_wx_member_expire_date where vip_type !='1' and pay_mode !='null' and  expireDate=date_sub('${hiveconf:expire}',7) and from_unixtime(endtime+28800,'yyyy-MM-dd')='${hiveconf:expire}'
) t1 -- 使用 ci_wx_member_expire_date where expireDate='2019-01-04'来获取用户到期时间为'2019-01-11'
inner join
(select title as channel,product_id from shencut_store_wx_marketing_channel ) t2
on t1.spread_origin= t2.product_id
) t3
left join
(
select
uid,
--type,
case when type='2' then '高级会员'
     when type='3' then 'VIP会员'
     when type='4' then '企业会员'
else '其他' end as renew_user_level ,
--open_type as  renew_time_type
case when open_type ='month' then '月付'
     when open_type ='year' then '年付'
else '其他' end as renew_time_type
from dbsync.shencut_store_wx_order where   status in ('20') and pay_type='1' and  (from_unixtime(inputtime+28800,'yyyy-MM-dd') between date_sub('${hiveconf:expire}',7) and date_add('${hiveconf:expire}',7))
) t4 -- 使用 shencut_store_wx_order where  from_unixtime(inputtime+28800,'yyyy-MM-dd') bewteen '2019-01-04' and '2019-01-18' 来获取相关的用户订单信息
on t3.uid= t4.uid
group by from_unixtime(t3.endtime+28800,'yyyy-MM-dd'),t3.channel,expire_time_type,expire_user_level,renew_time_type,renew_user_level;
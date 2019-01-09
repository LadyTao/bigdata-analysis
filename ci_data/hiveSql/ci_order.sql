use dbsync;
insert overwrite table mart.ci_order
select
t1.id,
t1.uid,
case when t1.pay_origin in ('3','4') then '神剪手移动端'
     when t1.pay_origin in ('1','2') then '神剪手'
else '其他' end as productLine,
t3.title as channel,
case when t1.open_type ='month' then '月付'
     when t1.open_type ='year' then '年付'
else '其他' end as subcribe_type,
'0' as product_version,
case when t1.`type`='2'  then '高级会员'
     when t1.`type`='3'  then 'VIP至尊会员'
     when t1.`type`='4'  then '企业会员'
else '其他'  end as member_classs,
case when t1.pay_origin in ('1','2') then 'Windows'
     when t1.pay_origin ='3' then 'IOS'
     when t1.pay_origin = '4' then 'Android'
else '其他'  end as os_platform,
case when t1.`from`='1' then '支付宝'
     when t1.`from`='2' then '微信'
     when t1.`from`='3' then '无需支付'
     when t1.`from`='4' then '小程序'
     when t1.`from`='5' then 'IOS支付'
else '其他' end as payment_pattern,
t1.order_no,
t1.amount,
t1.origin_amount,
t1.inputtime
from
(select
id,
uid,
order_no,
amount,
origin_amount,
open_type,
type,
`from`,
pay_origin,
inputtime
from dbsync.shencut_store_wx_order
where  status in ('10','20')
-- and FROM_UNIXTIME(inputtime) >'2019-01-07 17:05:50'
) t1
left join
dbsync.shencut_store_wx_member t2
on t1.uid= t2.uid
left join
dbsync.shencut_store_wx_marketing_channel t3
on t2.spread_origin  = t3.product_id;

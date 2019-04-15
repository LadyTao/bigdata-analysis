



use dbsync;
insert overwrite table mart.user_tag
select
t0.uid as id,
t0.*,
t3.country_cn,
t1.purchase_frequency,
t1.trade_status,
t1.last_effect_purchase_time
from
filmora_uuid_wx_member t0
inner join
(
SELECT t.uid ,
t.purchase_frequency,
case
	when t.purchase_frequency = 0 then 'C3 Registered No purchase (注册未购买)'
	when t.purchase_frequency =1 then 'C1 New Purchased User（新客,只下单一次）'
	when t.purchase_frequency >=2 then 'C2 Old Purchased User (老客，下过2单以上)'
	else 'others'
end AS trade_status,
-- 用户的订单数 还可以用来描述用户的交易状况
--t.days,
case
	when t.days BETWEEN 3 and 7 then '3 days ago'
	when t.days BETWEEN 7 and 10 then '7 days ago'
	when t.days BETWEEN 10 and 30 then '10 days ago'
	when t.days BETWEEN 30 and 90 then '1 month ago'
	when t.days BETWEEN 90 and 180 then '3 month ago'
	when t.days BETWEEN 180 and 365 then '6 month ago'
	when t.days >365 then '1 year ago'
	else 'other'
end as last_effect_purchase_time
from
(SELECT uid ,count( order_no)  as purchase_frequency,
-- 获取用户的订单数，做为计算购买频率的依据
datediff( from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime( max(CAST(inputtime  AS BIGINT)),'yyyy-MM-dd HH:mm:ss')) as  days
--求取用户所有下单中，时间戳最大的值作为用户最近一次的购买时间，与当前时间相减得出最近一次购买距离当前的天数
FROM filmora_es_wx_order_goods   where uid >0 GROUP BY uid) t
) t1
on (t0.uid= t1.uid)
left join
sync_ods_report_dm_dim_country_ods t3
on (upper(t3.country_en)= upper(t0.reg_country));


use dbsync;
insert overwrite table mart.order_clean
select
t1.id,
t1.uid,
'VP营销部' as marketdep,
t1.product_id,
t2.name,
t2.brand_name,
t2.language_name,
t2.platform,
t3.origin as payment,
t2.category_name as productline,
t3.country_short_name,
t3.license as license_type,
t5.channel_type,
t6.country_cn,
concat(t2.category_name,t4.version_name) as productVersion,
t1.amount,
t1.actual_amount,
t1.origin_amount,
t3.commission,
t3.tax,
t1.order_no,
t1.trade_no,
t1.inputtime,
tu.email,
tu.language,
tu.register_platform,
tu.register_time,
tu.activated_platform,
tu.license,
tu.effects_paid,
tu.subscribed_es,
tu.download_free_es,
tu.edm_subscribe_status,
ta.purchase_frequency,
ta.trade_status,
ta.last_effect_purchase_time
from
filmora_es_wx_order_goods t1
left join
cbs_out_product_detail t2
on (t1.product_id = t2.product_id )
left join
(SELECT
t9.order_id,
t9.product_id,
t9.version_id,
t9.order_num,
t9.license,
t10.short_name as country_short_name,
t9.origin,
t9.order_date,
t9.amount,
t9.commission,
t9.tax
from
cbs_out_order_info t9
join
cbs_out_sys_country t10
on UPPER(t9.country)= t10.country_name) t3
-- cbs_out_order_info t3
on (t1.trade_no = t3.order_num and t1.product_id = t3.product_id)
left join
cbs_out_product_version t4
on (t3.version_id = t4.version_id)
left join
cbs_local_main_order_channel_info t5
on (t5.order_id = t3.order_id)
left join sync_ods_report_dm_dim_country_ods t6
on (t6.country_en= t3.country_short_name)
left join
filmora_uuid_wx_member tu
on (t1.uid = tu.uid)
left join -- 从 filmora_es_wx_order_goods 获取购买频率,交易状态,最后一次资源订阅时间信息
(
SELECT t.uid ,
t.purchase_frequency,
case
	when t.purchase_frequency = 0 then 'C3 Registered No purchase (注册未购买)'
	when t.purchase_frequency =1 then 'C1 New Purchased User（新客,只下单一次）'
	when t.purchase_frequency >=2 then 'C2 Old Purchased User (老客，下过2单以上)'
	else 'others'
end AS trade_status,
--t.days,
case
	when t.days BETWEEN 3 and 7 then '3 days ago'
	when t.days BETWEEN 7 and 10 then '7 days ago'
	when t.days BETWEEN 10 and 30 then '10 days ago'
	when t.days BETWEEN 30 and 90 then '1 month ago'
	when t.days BETWEEN 90 and 180 then '3 month ago'
	when t.days BETWEEN 180 and 365 then '6 month ago'
	when t.days >365 then '1 year ago'
	else 'other'
end as last_effect_purchase_time
from
(SELECT uid ,count( order_no)  as purchase_frequency,
datediff( from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),from_unixtime( max(CAST(inputtime  AS BIGINT)),'yyyy-MM-dd HH:mm:ss')) as  days
FROM filmora_es_wx_order_goods   where uid >0 GROUP BY uid) t
) ta
on (t1.uid = ta.uid);

CREATE TABLE `ci_member_renew_rate_day` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `stat_date` date DEFAULT NULL COMMENT '数据日期，到期日期',
  `channel` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '用户推广渠道',
  `expire_time_type` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '到期用户vip类型：month,year',
  `expire_user_level` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '用户到期前的vip类型：高级，vip，企业',
  `renew_time_type` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '续费时长类型：month,year',
  `renew_user_level` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '续费vip类型：高级，vip，企业',
  `expire_user` int(10) DEFAULT NULL COMMENT '到期用户数',
  `renew_user` int(10) DEFAULT NULL COMMENT '续费用户数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Ci会员续费率表';


-- 查询语句
--------------------------
-- expire_time_type: month ,year (单选)
-- channel: 产品渠道 (支持多选)

--会员续费率(天)
select stat_date,
sum(renew_user) as total_renew_user ,
sum(renew_user)/sum(expire_user)  as total_rate,
sum(case when expire_user_level="高级会员" then renew_user else 0 end ) as senior_renew_user ,
sum(case when expire_user_level="高级会员" then renew_user else 0 end )/sum(case when expire_user_level = "高级会员" then expire_user else 0 end)  as senior_renew_rate,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end ) as vip_renew_user ,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end )/sum(case when expire_user_level = "VIP会员" then expire_user else 0 end)  as vip_renew_rate,
sum(case when expire_user_level="企业会员" then renew_user else 0 end ) as enter_renew_user ,
sum(case when expire_user_level="企业会员" then renew_user else 0 end )/sum(case when expire_user_level = "企业会员" then expire_user else 0 end)  as enter_renew_rate
from ci_member_renew_rate_day

-- where channel in () ,expire_time_type = '' -- 需要传入下拉项相关的channel,expire_time_type 信息
group by stat_date


-- 会员续费率(月)
--会员续费率(天)
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
sum(renew_user) as total_renew_user ,
-- sum(expire_user) as total_expire_user,
sum(renew_user)/sum(expire_user)  as total_rate,
sum(case when expire_user_level="高级会员" then renew_user else 0 end ) as senior_renew_user ,
-- sum(case when expire_user_level="高级会员" then expire_user else 0 end) as senior_expire_user,
sum(case when expire_user_level="高级会员" then renew_user else 0 end )/sum(case when expire_user_level = "高级会员" then expire_user else 0 end)  as senior_renew_rate,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end ) as vip_renew_user ,
-- sum(case when expire_user_level="VIP会员" then expire_user else 0 end)  as vip_expire_user,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end )/sum(case when expire_user_level = "VIP会员" then expire_user else 0 end)  as vip_renew_rate,
sum(case when expire_user_level="企业会员" then renew_user else 0 end ) as enter_renew_user ,
-- sum(case when expire_user_level="企业会员" then expire_user else 0 end) as company_expire_user,
sum(case when expire_user_level="企业会员" then renew_user else 0 end )/sum(case when expire_user_level = "企业会员" then expire_user else 0 end)  as enter_renew_rate
from ci_member_renew_rate_day

-- where channel in () ,expire_time_type = ''  -- 需要传入相关的channel,expire_time_type 信息
group by DATE_FORMAT(stat_date,'%Y-%m')

-- 同类型会员续费率(天)
select
stat_date,
sum(renew_user) as total_renew_user ,
-- sum(expire_user) as total_expire_user,
sum(renew_user)/sum(expire_user)  as total_rate,
sum(case when expire_user_level="高级会员" then renew_user else 0 end ) as senior_renew_user ,
-- sum(case when expire_user_level="高级会员" then expire_user else 0 end) as senior_expire_user,
sum(case when expire_user_level="高级会员" then renew_user else 0 end )/sum(case when expire_user_level = "高级会员" then expire_user else 0 end)  as senior_renew_rate,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end ) as vip_renew_user ,
-- sum(case when expire_user_level="VIP会员" then expire_user else 0 end)  as vip_expire_user,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end )/sum(case when expire_user_level = "VIP会员" then expire_user else 0 end)  as vip_renew_rate,
sum(case when expire_user_level="企业会员" then renew_user else 0 end ) as enter_renew_user ,
-- sum(case when expire_user_level="企业会员" then expire_user else 0 end) as company_expire_user,
sum(case when expire_user_level="企业会员" then renew_user else 0 end )/sum(case when expire_user_level = "企业会员" then expire_user else 0 end)  as enter_renew_rate
from ci_member_renew_rate_day
where expire_user_level=renew_user_level
-- where channel in () ,expire_time_type = ''  -- 需要传入相关的channel,expire_time_type 信息
group by stat_date

--同类型会员续费率(月)
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
sum(renew_user) as total_renew_user ,
-- sum(expire_user) as total_expire_user,
sum(renew_user)/sum(expire_user)  as total_rate,
sum(case when expire_user_level="高级会员" then renew_user else 0 end ) as senior_renew_user ,
-- sum(case when expire_user_level="高级会员" then expire_user else 0 end) as senior_expire_user,
sum(case when expire_user_level="高级会员" then renew_user else 0 end )/sum(case when expire_user_level = "高级会员" then expire_user else 0 end)  as senior_renew_rate,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end ) as vip_renew_user ,
-- sum(case when expire_user_level="VIP会员" then expire_user else 0 end)  as vip_expire_user,
sum(case when expire_user_level="VIP会员" then renew_user else 0 end )/sum(case when expire_user_level = "VIP会员" then expire_user else 0 end)  as vip_renew_rate,
sum(case when expire_user_level="企业会员" then renew_user else 0 end ) as enter_renew_user ,
-- sum(case when expire_user_level="企业会员" then expire_user else 0 end) as company_expire_user,
sum(case when expire_user_level="企业会员" then renew_user else 0 end )/sum(case when expire_user_level = "企业会员" then expire_user else 0 end)  as enter_renew_rate
from ci_member_renew_rate_day
where expire_user_level=renew_user_level -- and channel in () ,expire_time_type = ''   -- 需要传入相关的channel,expire_time_type 信息
group by DATE_FORMAT(stat_date,'%Y-%m')

-- 查询饼图

-- 月表统计
--2019-12 会员变更的订阅类型分布；月付,年付在外圈，(会员类型：高级会员,VIP会员,企业会员)在内圈
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
renew_time_type,
renew_user_level,
sum(renew_user)
from
ci_member_renew_rate_day
where channel in ('百度推广','360推广') and expire_time_type ='月付' and DATE_FORMAT(stat_date,'%Y-%m') = '{month_var}'
group by  DATE_FORMAT(stat_date,'%Y-%m'),renew_time_type,renew_user_level

--2019-12 会员变更的会员类型分布；月付,年付在内圈，(会员类型：高级会员,VIP会员,企业会员)在外圈
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
renew_user_level,
renew_time_type,
sum(renew_user)
from
ci_member_renew_rate_day
where channel in ('百度推广','360推广') and expire_time_type ='月付' and DATE_FORMAT(stat_date,'%Y-%m') = '2019-01'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(月)"查询参数一致，另外需要添加新的参数每条数据的日期如：'2019-01'
group by  DATE_FORMAT(stat_date,'%Y-%m'),renew_user_level,renew_time_type
-- 日表统计
-- 2019-01-12 会员变更的订阅类型分布；月付,年付在外圈，(会员类型：高级会员,VIP会员,企业会员)在内圈
select
stat_date,
renew_time_type,
renew_user_level
sum(renew_user)
from
ci_member_renew_rate_day
where channel in ('百度推广','360推广') and expire_time_type ='月付' and stat_date ='{day_var}'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(日)"查询参数一致，另外需要添加新的参数每条数据的日期
group by  stat_date,,renew_time_type,renew_user_level


--2019-01-12 会员变更的会员类型分布；月付,年付在内圈，(会员类型：高级会员,VIP会员,企业会员)在外圈
select
stat_date,
renew_user_level,
renew_time_type,
sum(renew_user)
from
ci_member_renew_rate_day
where channel in ('百度推广','360推广') and expire_time_type ='月付' and stat_date = '{day_var}'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(日)"查询参数一致，另外需要添加新的参数每条数据的日期
group by  stat_date,renew_user_level,renew_time_type
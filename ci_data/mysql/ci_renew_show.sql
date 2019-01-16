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


-- 查询饼图

-- 月表统计
--2019-12
-- 点击"总体续费人数"进入
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
renew_time_type,
renew_user_level,
sum(renew_user)
from
ci_member_renew_rate_day
where  channel in ('百度推广','360推广') and expire_time_type ='月付' and DATE_FORMAT(stat_date,'%Y-%m') ='2019-01'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(月)"查询参数一致，另外需要添加新的参数每条数据的月份参数，
group by  DATE_FORMAT(stat_date,'%Y-%m'),renew_time_type,renew_user_level


-- expire_user_level {会员类型}'=可选：高级会员,VIP会员,企业会员
-- 点击"高级会员续费人数"进入 ，需要where条件中expire_user_level='高级会员'
-- 点击"VIP会员续费人数"进入 ，需要where条件中expire_user_level='VIP会员'
-- 点击"企业会员续费人数"进入 ，需要where条件中expire_user_level='企业会员'
select
DATE_FORMAT(stat_date,'%Y-%m') as stat_month,
renew_time_type,
renew_user_level,
sum(renew_user)
from
ci_member_renew_rate_day
where expire_user_level= " " and channel in ('百度推广','360推广') and expire_time_type ='月付' and DATE_FORMAT(stat_date,'%Y-%m') ='2019-01'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(月)"查询参数一致，另外需要添加新的参数每条数据的月份参数，以及 expire_user_level 参数
group by  DATE_FORMAT(stat_date,'%Y-%m') ,renew_time_type,renew_user_level



-- 2019-01-12  日表饼图
-- 点击"总体续费人数"进入
select
stat_date,
renew_time_type,
renew_user_level,
sum(renew_user)
from
ci_member_renew_rate_day
where channel in ('百度推广','360推广') and expire_time_type ='月付' and stat_date ='2019-01-12'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(日)"查询参数一致，另外需要添加新的参数每条数据的日期参数
group by  stat_date,renew_time_type,renew_user_level


-- expire_user_level {会员类型}'=可选：高级会员,VIP会员,企业会员
-- 点击"高级会员续费人数"进入 ，需要where条件中expire_user_level='高级会员'
-- 点击"VIP会员续费人数"进入 ，需要where条件中expire_user_level='VIP会员'
-- 点击"企业会员续费人数"进入 ，需要where条件中expire_user_level='企业会员'
select
stat_date,
renew_time_type,
renew_user_level,
sum(renew_user)
from
ci_member_renew_rate_day
where expire_user_level= " " and channel in ('百度推广','360推广') and expire_time_type ='月付' and stat_date ='2019-01-12'
-- where 条件中channel ,expire_time_type 的参数跟"会员续费率(日)"查询参数一致，另外需要添加新的参数每条数据的日期参数，以及 expire_user_level 参数
group by  stat_date,renew_time_type,renew_user_level

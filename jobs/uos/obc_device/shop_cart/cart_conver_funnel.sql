select
	-- 维度
	"filmstocks" as produce,
	tb0.day,
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
	(select
		case when cid=10701 then "Filmstocks英语站"
		when cid=10702 then "Filmstocks法语站"
		when cid=10703 then "Filmstocks葡萄牙语站"
		when cid=10704 then "Filmstocks西班牙语站"
		when cid=10705 then "Filmstocks德语站"
		when cid=10706 then "Filmstocks日语站"
		when cid=10707 then "Filmstocks意大利语站" end as site_type,
		count(if(url like "%plan.html", wsid, null)) as plan_num,
		count(distinct if(url like "%plan.html", wsid, null)) as plan_user_num,
		count(if(url like "%/pay/checkout.html%", wsid, null)) as checkout_num,
		count(distinct if(url like "%/pay/checkout.html%", wsid, null)) as checkout_user_num,
		day
	from log.wl_filmstocks_com
	where day="2019-04-16" and cid in (10701, 10702, 10703, 10704, 10705, 10706, 10707) and (url like "%plan.html" or url like "%/pay/checkout.html%")
	group by day, case when cid=10701 then "Filmstocks英语站"
		when cid=10702 then "Filmstocks法语站"
		when cid=10703 then "Filmstocks葡萄牙语站"
		when cid=10704 then "Filmstocks西班牙语站"
		when cid=10705 then "Filmstocks德语站"
		when cid=10706 then "Filmstocks日语站"
		when cid=10707 then "Filmstocks意大利语站" end) tb0
JOIN
	(select
		FROM_UNIXTIME(inputtime, "yyyy-MM-dd") as day,
		case when from_site="24" then "Filmstocks英语站"
		when from_site="25" then "Filmstocks法语站"
		when from_site="26" then "Filmstocks葡萄牙语站"
		when from_site="27" then "Filmstocks西班牙语站"
		when from_site="28" then "Filmstocks德语站"
		when from_site="29" then "Filmstocks日语站"
		when from_site="30" then "Filmstocks意大利语站" end as site_type,
		count(*) as generate_num,
		count(distinct if(status in ("10", "20"), order_no, null)) as succeed_num,
		count(distinct if(status="40", order_no, null)) as falied_num,
		count(distinct if(status in ("1", "50"),order_no,null)) as unpay_num
	from dbsync.f_order_wx_order
	where origin="2" and FROM_UNIXTIME(inputtime, "yyyy-MM-dd")="2019-04-16" and from_site in ("24","25","26","27","28","29","30")
	group by FROM_UNIXTIME(inputtime, "yyyy-MM-dd"),
		case when from_site="24" then "Filmstocks英语站"
		when from_site="25" then "Filmstocks法语站"
		when from_site="26" then "Filmstocks葡萄牙语站"
		when from_site="27" then "Filmstocks西班牙语站"
		when from_site="28" then "Filmstocks德语站"
		when from_site="29" then "Filmstocks日语站"
		when from_site="30" then "Filmstocks意大利语站" end) tb1
on tb0.day=tb1.day and tb0.site_type=tb1.site_type
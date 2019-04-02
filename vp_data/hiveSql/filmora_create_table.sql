-- 待导入hive的数据：




-- 1

-- order_info :order_id,order_num,country_short_name,origin,license,version_id,order_date
SELECT
order_id,
order_num,
country_short_name,
origin,
license,
version_id,
order_date
from order_info
where LENGTH(country_short_name)>0 and LENGTH(order_num)>0 and LENGTH(license)>0


CREATE TABLE IF NOT EXISTS cbs_out_order_info(
	order_id INT COMMENT'order id',
	order_num STRING COMMENT'order no.',
	version_id INT COMMENT  'product vesion id',
	license STRING COMMENT 'license type',
	country_short_name STRING COMMENT 'customer country',
	origin STRING COMMENT 'pay platform',
	order_date STRING  COMMENT 'order date'
	)
COMMENT 'filmora order_info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_order_info';


-- 2
-- product_info: product_id,name,brand_id,language_id,product_num
SELECT
product_id,name,brand_id,language_id,product_num
from product_info
WHERE product_id>700


CREATE TABLE IF NOT EXISTS cbs_out_product_info(
	product_id INT COMMENT'order id',
	name STRING COMMENT'order no.',
	brand_id INT COMMENT  'brand id,1:Wondershare ,2:Aimersoft,3:iSkysoft,4:other',
	language_id INT COMMENT 'language_id',
	category_id int comment 'product line',
	product_num STRING COMMENT 'product  numbers 截取第一位 1 Win 2 Mac 3 Mobile 5 其他   4和6废弃'
	)
COMMENT 'filmora product_info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_product_info';







-- 3
-- product_version:  version_id, version_name
SELECT
version_id,
version_name
FROM product_version

CREATE TABLE IF NOT EXISTS cbs_out_product_version (
		 version_id INT COMMENT 'product version id',
		 version_name STRING COMMENT 'product version name'
	)
	COMMENT 'filmora product_version info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_product_version';


--4 product_type
-- category_id, category_name

CREATE TABLE IF NOT EXISTS cbs_out_product_type (
		 category_id INT COMMENT 'product category id',
		 category_name STRING COMMENT 'product category name'
	)
	COMMENT 'filmora product category(productLine) info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_product_type';



-- 5
-- sys_brand :  brand_id,brand_name
1	Wondershare
2	Aimersoft
3	iSkysoft

CREATE TABLE IF NOT EXISTS cbs_out_sys_brand (
		 brand_id INT COMMENT 'product brand id',
		 brand_name STRING COMMENT 'product brand name'
	)
	COMMENT 'filmora product brand info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_sys_brand';



-- 6
-- sys_language:  language_id,language_name
SELECT language_id,language_name FROM sys_language


CREATE TABLE IF NOT EXISTS cbs_out_sys_language (
		 language_id INT COMMENT ' language_id',
		 language_name STRING COMMENT 'language_name'
	)
COMMENT 'filmora sys_language info from the cbs_out'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_out_sys_language';


-- 7
-- dim_country_ods:country_en,country_cn
CREATE TABLE IF NOT EXISTS sync_ods_report_dm_dim_country_ods (
		 country_en string COMMENT ' country short name in English',
		 country_cn STRING COMMENT 'country name in Chinese'
	)
COMMENT 'the map between country_short_name and country name in Chinese'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/sync_ods_report_dm_dim_country_ods';



-- 8
-- wx_order_goods : id,uid,product_id,type,title,amount,actual_amount,origin_amount,order_no,trade_no,pay_type,inputtime
SELECT
id,
uid,
product_id,
type,
title,
amount,
actual_amount,
origin_amount,
order_no,
trade_no,
pay_type,
inputtime
from
wx_order_goods
WHERE pay_type =1 and uid >0 and product_id  > 0


CREATE TABLE IF NOT EXISTS filmora_es_wx_order_goods(
	id INT COMMENT 'id',
	uid INT COMMENT 'user id',
	product_id INT COMMENT 'product id',
    type INT COMMENT 'buy tyep 1:resource package, 2:filmora license',
	title string COMMENT 'price',
	amount DECIMAL(20,2) COMMENT 'the us dollor price',
	actual_amount DECIMAL(20,2) comment 'actual price',
	origin_amount  DECIMAL(20,2) comment 'origin price',
	order_no string comment 'order no',
	trade_no string comment 'order no in the cbs',
	pay_type int comment '1:paid 10:gift,20:Activities gif',
	inputtime string comment 'order date'
	)
COMMENT 'filmora wx_order_goods from the fimlora_es'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/filmora_es_wx_order_goods';


-- 9
-- order_channel_info : order_id ,channel_type
SELECT
order_id,
channel_type,
from order_channel_info


-- order_id, channel_type
CREATE TABLE  IF NOT EXISTS cbs_local_main_order_channel_info (
		 order_id INT COMMENT 'order id',
		 channel_type STRING COMMENT 'channel type'
	)
COMMENT 'filmora order_channel_info from the cbs_local_main'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/cbs_local_main_order_channel_info';


-- 10
-- wx_member_*：
select uid,email,username,language,register_platform,register_time,activated_platform,activated_platform_bit,license,effects_paid,subscribed_es,download_free_es,is_old_member,last_visit,inputtime,updatetime,reg_country,from_brand,edm_subscribe_status,from_site,,,
(SELECT * FROM wx_member_1
UNION
SELECT * FROM wx_member_2
UNION
SELECT * FROM wx_member_3
UNION
SELECT * FROM wx_member_4
UNION
SELECT * FROM wx_member_5
UNION
SELECT * FROM wx_member_6
UNION
SELECT * FROM wx_member_7
UNION
SELECT * FROM wx_member_8
UNION
SELECT * FROM wx_member_9) t1


CREATE TABLE  if not exists filmora_uuid_wx_member (
  id int ,
  uid int COMMENT '用户UID（账户系统）',
  email string ,
  username string ,
  language int COMMENT '语言',
  register_platform string COMMENT '注册平台',
  register_time int COMMENT '注册时间',
  activated_platform int COMMENT '使用平台',
  activated_platform_bit int COMMENT '使用平台的二进制表示,{mac,win,fxs,io,go,scrn,android-go,ios-go,android-vlog, ios-vlog}',
  license  int COMMENT '是否购买有效Lincense( 1: Paid , 2：No)',
  effects_paid  int COMMENT '是否购买了非免费的特效（1 ：Yes ； 2： No）',
  subscribed_es int COMMENT '订阅过资源商城  1 ： 是, 2 :否',
  download_free_es  int COMMENT '下载过免费特效的用户  1 ： 是, 2 :否',
  is_old_member int COMMENT '是否注册过member_id账户 1 ： 是, 2 :否',
  last_visit int COMMENT '最后访问时间',
  inputtime int,
  updatetime int,
  reg_country string COMMENT '注册国家',
  from_brand  string COMMENT '注册品牌来源： 1. wondershare 2. iskysoft',
  edm_subscribe_status int COMMENT 'EDM订阅状态：1 已订阅， 2 未订阅',
  from_site int COMMENT '站点来源'
	)
COMMENT 'filmora  wx_member from the filmora_uuid'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/filmora_uuid_wx_member';


use dbsync;
-- store.wx_member
CREATE TABLE IF NOT EXISTS shencut_store_wx_member(
  id int  ,
  uid int COMMENT ' DEFAULT "0"  用户UID（账户系统）',
  vip_type string COMMENT '用户组（1：免费用户，2：高级用户，3：VIP用户，4：企业用户）',
  endtime int   COMMENT '会员到期时间',
  status string  COMMENT '1：正常；2：锁定',
  base_info string  COMMENT '用户基础信息，由wondershareID返回过来的数据',
  mobile string DEFAULT '' COMMENT '手机号码',
  nickname string COMMENT '昵称',
  reg_ip string COMMENT '注册ip',
  reg_location string COMMENT '注册地区',
  reg_origin string  COMMENT '注册来源 1:web端,2:产品端,3:IOS端,4:安卓端',
  spread_origin int COMMENT '渠道推广来源',
  inputtime int comment '0',
  updatetime int comment '0',
  pay_mode string COMMENT '付费方式:day,year,month',
  industry string COMMENT '行业',
  interest string COMMENT '用户兴趣'
)
COMMENT '神剪手会员表 store.wx_member'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member';

-- tinyint(1)字段加载到hive后，数据丢失 ，解决办法： tinyint(1) casez转化成string
-- 官方手册的解决办法;tinyInt1isBit=false 或者： --map-column-java  vip_type=java.lang.Integer,status=java.lang.Integer,reg_origin=java.lang.Integer,industry=java.lang.Integer 不能用
--map-column-hive  vip_type=STRING,status=STRING,reg_origin=STRING,industry=STRING

-- 检查mysql中tinyint(1) 类型是否成功转化为string
select distinct vip_type from dbsync.shencut_store_wx_member;
select distinct status from dbsync.shencut_store_wx_member;
select distinct reg_origin from dbsync.shencut_store_wx_member;
select distinct industry from dbsync.shencut_store_wx_member;
--用户UID 手机号  会员等级  注册时间  到期时间 订单次数  用户来源 推广渠道 行业 兴趣



--stroe.wx_order
CREATE TABLE shencut_store_wx_order (
  id int   ,
  uid int   comment 'uid default:0',
  subject string    COMMENT '主题',
  type string    COMMENT '交易类型（会员充值类：【2：充值高级会员， 3：充值VIP会员，4：充值企业用户】请保持与member表的group字段一致）',
  trade_no string    COMMENT '交易单号',
  amount decimal(10,2)    COMMENT '交易金额',
  origin_amount decimal(10,2)    COMMENT '原价',
  `from` string     COMMENT '支付方式（1：支付宝，2：微信，3：无需支付 4： 小程序，5：IOS支付）',
  pay_origin string     COMMENT '支付来源（1：Web，2：PC，3：IOS，4：安卓）',
  order_no string    COMMENT '订单号',
  open_type string    COMMENT '付费方式:day,year,month',
  extra string  COMMENT '附加信息 JSON {open_vip_time: 开通VIP时间, ''open_type'': 开通类型, ''username'': 用户名, ''endtime'': 截止时间}',
  status string     COMMENT '订单状态 (1:待支付，10：代付款，20：支付成功)',
  pay_type string     COMMENT '付费类型（1:正常支付，10：手动赠送，20: 活动赠送）',
  inputtime int ,
  updatetime int,
  open_vip_type string COMMENT '开通VIP类型，1：初次开通，2：续费VIP，3：升级VIP'
)
COMMENT '神剪手订单表 store'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_order';

-- 检查mysql中tinyint(1) 类型是否成功转化为string
 --map-column-hive type=STRING,from=STRING,pay_origin=STRING,status=STRING,pay_type=STRING,open_vip_type=STRING
select distinct pay_origin from dbsync.shencut_store_wx_order;
select distinct type from dbsync.shencut_store_wx_order;
select distinct from from dbsync.shencut_store_wx_order;
select distinct status from dbsync.shencut_store_wx_order ;
select distinct pay_type from dbsync.shencut_store_wx_order ;
select distinct open_vip_type from dbsync.shencut_store_wx_order ;


CREATE TABLE shencut_store_wx_marketing_channel (
  id INT,
  siteid SMALLINT,
  title STRING COMMENT '渠道名称',
  listorder SMALLINT  ,
  creator STRING ,
  template STRING  ,
  status STRING  ,
  inputtime STRING COMMENT  '1970-01-01 08:00:01',
  updatetime STRING COMMENT  '1970-01-01 08:00:01',
  download_url STRING  ,
  landing_page STRING  ,
  install_package STRING ,
  cdn_status STRING  ,
  cdn_task_id INT   ,
  package_version STRING  ,
  slug STRING ,
  product_id INT  COMMENT 'wx_member表中的spread_origin',
  install_jump_url STRING  ,
  is_give_vip STRING  ,
  vip_type STRING ,
  open_type STRING COMMENT 'month',
  open_time_unit STRING  COMMENT 'day',
  open_time STRING  COMMENT'open time',
  download_url_mobile STRING COMMENT'download url',
  landing_page_mobile STRING COMMENT '',
  install_package_apk STRING COMMENT ''
)
COMMENT '神剪手渠道信息表 store'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_marketing_channel';

SELECT DISTINCT status from dbsync.shencut_store_wx_marketing_channel
SELECT DISTINCT cdn_status from dbsync.shencut_store_wx_marketing_channel
SELECT DISTINCT is_give_vip from dbsync.shencut_store_wx_marketing_channel
SELECT DISTINCT vip_type from dbsync.shencut_store_wx_marketing_channel
SELECT DISTINCT open_time from dbsync.shencut_store_wx_marketing_channel


--mysql tinyint(1) --> hive STRING：status=STRING,cdn_status=STRING, is_give_vip=STRING,vip_type=STRING,open_time=STRING


-- wx_member_interest_tags

CREATE TABLE shencut_store_wx_member_interest_tags (
 id int,
 tag STRING COMMENT '标签',
 name STRING COMMENT '标签名',
 status INT COMMENT '状态（1：可用，2：禁用）'
)
COMMENT '神剪手会员兴趣标签说明表 store'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://hdp-0.local:8020/path/dbsync/shencut_store_wx_member_interest_tags';





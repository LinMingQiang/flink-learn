insert into test2
select rowkey, ROW(mid, deviceid, oaid, oaidMd5, openid, xbOrder)
from
(
 select
 cast(t1.vipruid as string) as rowkey,
   t1.mid,
 coalesce(t2.info.deviceid, '') as deviceid,
 coalesce(t2.info.oaid, '') as oaid,
 coalesce(t2.info.oaidMd5, '') as oaidMd5,
 coalesce(t2.info.openid, '') as openid,
  coalesce(t2.info.xbOrder, '') as xbOrder
 from test /*+ OPTIONS('properties.group.id'='PRODUCTION_queue_dmp_clean_mars_sc_mobile_page_logger_clean_flink_v2','scan.startup.mode' = 'group-offsets') */ as t1
 left join hive.vipflink.ad_t_uid for SYSTEM_TIME as of PROCTIME() as t2
 on t2.rowkey = cast(t1.vipruid as string)
 where t1.mid is not null  and t1.vipruid is not null )
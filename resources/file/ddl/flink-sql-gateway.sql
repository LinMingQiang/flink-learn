CREATE TABLE test (
  `msg` VARCHAR,
  `money` BIGINT
 ) WITH (
'connector' = 'kafka',
'topic' = 'test',
'properties.bootstrap.servers' = 'localhost:9092',
'properties.group.id' = 'test',
'scan.startup.mode' = 'latest-offset',
'format' = 'json',
'json.ignore-parse-errors' = 'true');

CREATE TABLE test2 (
  `msg` VARCHAR,
    `money` BIGINT
 ) WITH (
   'connector' = 'kafka',
     'topic' = 'test2',
     'properties.bootstrap.servers' = 'localhost:9092',
     'properties.group.id' = 'test',
     'scan.startup.mode' = 'latest-offset',
     'format' = 'json',  'json.ignore-parse-errors' = 'true');
CREATE TABLE test3 (
  `msg` VARCHAR,
    `money` BIGINT
     ) WITH (
       'connector' = 'kafka',
       'topic' = 'test2',
       'properties.bootstrap.servers' = 'localhost:9092',
       'properties.group.id' = 'test',
       'scan.startup.mode' = 'latest-offset',
       'format' = 'json',  'json.ignore-parse-errors' = 'true');
select msg,count(money) as money from  test /*+  OPTIONS('properties.group.id'='test','scan.startup.mode' = 'group-offsets') */ group by msg;
insert into test3 select msg,money as money from  test;
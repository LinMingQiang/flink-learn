CREATE TABLE backflow_count (
    hllCacheKey VARCHAR,
    backflowCountUV VARBINARY
) WITH (
    'connector.type' = 'redis', -- 维表存储
    'connector.hostport' = '${redis.hostport}',
    'connector.password' = '${redis.password}'
);
----是  是是
SET aa=bb;
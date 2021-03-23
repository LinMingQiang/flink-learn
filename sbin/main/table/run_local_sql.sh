#flink savepoint 72b06ec4fb8b664478da502edcbc7dee savepath -yid application_1575610054801_0312
# /Users/eminem/programe/flink-1.9.1/bin/flink savepoint c4fb2136dbfd8bf66d20be5dab04a647 /Users/eminem/workspace/flink/flink-learn/savepoint
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-ff6acb-37823a69f146
#http://localhost:8081/#/overview
#-s  file:/Users/eminem/workspace/flink/flink-learn/checkpoint/WordCountEntry/202103221803/a66a8dd5b333e5d83e6a720c2a737318/chk-1 \

echo "start ........"
flink run \
-s file:/Users/eminem/workspace/flink/flink-learn/checkpoint/WordCountEntry/202103230903/c1c9174bd507a6ccbd0b95bf931dcc7d/chk-1 \
-c com.flink.streamsql.entry.FlinkSQLFactoryEntry \
/Users/eminem/workspace/flink/flink-learn/flink-sql/target/flink-sql-1.0.0.jar \
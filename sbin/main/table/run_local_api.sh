#flink savepoint 72b06ec4fb8b664478da502edcbc7dee savepath -yid application_1575610054801_0312
# /Users/eminem/programe/flink-1.9.1/bin/flink savepoint c4fb2136dbfd8bf66d20be5dab04a647 /Users/eminem/workspace/flink/flink-learn/savepoint
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-ff6acb-37823a69f146
#http://localhost:8081/#/overview
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-c4fb21-d0e5d7045b40 \

echo "start ........"
flink run -c com.flink.streamtable.entry.FlinkStreamTableApi \
/Users/eminem/workspace/flink/flink-learn/flink-sql/target/flink-sql-1.0.0.jar \
"runTemJoin"
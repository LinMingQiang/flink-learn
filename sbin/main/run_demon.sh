#flink savepoint 72b06ec4fb8b664478da502edcbc7dee savepath -yid application_1575610054801_0312
# /Users/eminem/programe/flink-1.9.1/bin/flink savepoint c4fb2136dbfd8bf66d20be5dab04a647 /Users/eminem/workspace/flink/flink-learn/savepoint
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-ff6acb-37823a69f146
#http://localhost:8081/#/overview
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-c4fb21-d0e5d7045b40 \
#flink savepoint b530724c897463ace25054491c67d085 file:///Users/eminem/workspace/flink/flink-learn/checkpoint/savepoint/
echo "start ........"

#flink run -s file:///Users/eminem/workspace/flink/flink-learn/checkpoint/SocketJavaPoJoWordcountTest/202007011607/fbcebbb548986ed453ca5a8a85554c9b/chk-7 \
#-c com.flink.learn.stateprocessor.SocketJavaPoJoWordcountTest \
#/Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar

flink run -s file:///Users/eminem/workspace/flink/flink-learn/checkpoint/tanssavepoint \
-c com.flink.learn.stateprocessor.SocketJavaPoJoWordcountTest \
/Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar

#flink run -c com.flink.learn.stateprocessor.SocketJavaPoJoWordcountTest /Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
#flink run -c com.flink.learn.entry.KafkaWordCountTest /Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
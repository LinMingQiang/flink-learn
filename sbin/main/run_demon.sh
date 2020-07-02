#flink savepoint 72b06ec4fb8b664478da502edcbc7dee savepath -yid application_1575610054801_0312
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-ff6acb-37823a69f146
#http://localhost:8081/#/overview
#-s file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-c4fb21-d0e5d7045b40 \
#flink savepoint b530724c897463ace25054491c67d085 file:///Users/eminem/workspace/flink/flink-learn/checkpoint/savepoint/
echo "start ........"

flink run -s file:///Users/eminem/workspace/flink/flink-learn/checkpoint/SocketScalaWordcountTest/savepoint/savepoint-4574f7-a7873e4cd50f \
-c com.flink.learn.stateprocessor.SocketScalaWordcountTest \
/Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
#flink run -s file:///Users/eminem/workspace/flink/flink-learn/checkpoint/tanssavepoint \
#-c com.flink.learn.stateprocessor.SocketJavaPoJoWordcountTest \
#/Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar

#flink savepoint 4574f7a8329a5054f81cf8716a0c948b file:///Users/eminem/workspace/flink/flink-learn/checkpoint/SocketScalaWordcountTest/savepoint

#flink run -c com.flink.learn.stateprocessor.SocketScalaWordcountTest /Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
#flink run -c com.flink.learn.stateprocessor.SocketJavaPoJoWordcountTest /Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
#flink run -c com.flink.learn.entry.KafkaWordCountTest /Users/eminem/workspace/flink/flink-learn/flink-demo/target/flink-demo-1.0.0.jar
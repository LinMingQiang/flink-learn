#! /bin/sh
# -p 并行度 5
# -yn taskmanager  yarn 的 container 的数量 4
# -yjm JobManager 内存
# -ytm taskManager 内存
# -ys 每个 taskManager 的 slot数量 2
# -s hdfs://Stream2/user/marketplus/ssp-flink/checkpoint/806c6e5744ac8e3cbd2d2afdc0a09cf6/chk-4
#/home/marketplus/flink-1.9.0/bin/flink savepoint 1903a7d381e51f429b47dd0cea63ff7a hdfs://Stream2/user/marketplus/ssp-flink/savepoint -yid application_1575610054801_0318
flink run \
 -c com.flink.pro.entry.FlinkStreamReport2MysqlEntry \
 /Users/eminem/workspace/flink/flink-pro-demo/dist/lib/stream-v1.0.0.jar \
/Users/eminem/workspace/flink/flink-pro-demo/dist/conf/application.properties
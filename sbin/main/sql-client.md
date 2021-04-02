如何在Yarn-session中使用 sql-client

正常情况下 sql-client.sh 都是在flink集群里面，但是如果我们没有flink集群只有yarn怎么办

1：开启一个yarn-session
./bin/yarn-session.sh -tm 2192 -s 8 -jm 1024

2：增加一个配置文件 vi conf/sql-client-session.yaml
添加
configuration:
execution.target: yarn-session
yarn.application.id: application_1594985361863_0512

3:启动sql-client
./bin/sql-client.sh embedded -e conf/sql-client-session.yaml




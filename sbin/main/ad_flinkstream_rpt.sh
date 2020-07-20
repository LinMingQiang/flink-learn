#! /bin/sh
set -x -e
export LANG=en_US.UTF-8
MOBSSP_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
source ${MOBSSP_HOME}/conf/pro-init.sh
javaopt="-Dlog4j.configuration=file:./log4j.properties"
echo $javaopt
# -p 并行度 5
# -yn taskmanager  yarn 的 container 的数量 4
# -yjm JobManager 内存
# -ytm taskManager 内存
# -ys 每个 taskManager 的 slot数量 2
# -s hdfs://Stream2/user/marketplus/ssp-flink/checkpoint/806c6e5744ac8e3cbd2d2afdc0a09cf6/chk-4
#/home/marketplus/flink-1.9.0/bin/flink savepoint 1903a7d381e51f429b47dd0cea63ff7a hdfs://Stream2/user/marketplus/ssp-flink/savepoint -yid application_1575610054801_0318
/home/marketplus/flink-1.9.0/bin/flink run \
 -m yarn-cluster  \
 --yarnname AdFlink_RTP_${MOBSSP_ENV} \
 -yD env.java.opts=$javaopt \
 --yarnqueue marketplus \
 -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 \
 -c com.mob.adplat.entry.FlinkStreamReport2MysqlEntry \
 $stream_jar $profilePath
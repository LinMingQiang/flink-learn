#! /bin/sh
set -x -e
export LANG=en_US.UTF-8
PRO_HOME="$(
  cd "$(dirname "$0")"/../../..
  pwd
)"
source ${PRO_HOME}/conf/job_init.sh
#./yarn-session.sh -tm 2192 -s 20 -jm 1024
#/home/marketplus/flink-1.13.0/bin/yarn-session.sh -tm 2192 -s 20 -jm 1024
# https://mvnrepository.com/artifact/org.apache.flink/flink-shaded-hadoop-2-uber
#application mode ，可以在main函数里面多次执行execute
/home/marketplus/flink-1.13.0/bin/flink run \
  -t yarn-per-job --detached \
  --yarnname Flink-Learn \
  -p 5 -yjm 1024 -ytm 1024 -ys 2 \
  -c com.flink.learn.entry.WordCountTest \
  $core_jar

# /home/marketplus/flink-1.13.0/bin/flink run-application  \
# -t yarn-application \
# -Djobmanager.memory.process.size=2048m \
# -Dtaskmanager.memory.process.size=4096m \
# -Dtaskmanager.numberOfTaskSlots=2 \
# -Dparallelism.default=10 \
# -Dyarn.application.name="MyFlinkApp" \
# -Dyarn.provided.lib.dirs="hdfs://Stream2/tmp/flink-dist/flink-1.13.0" \
#  -c com.flink.learn.entry.WordCountTest \
#  hdfs://Stream2/tmp/flink-core-1.13.0.jar

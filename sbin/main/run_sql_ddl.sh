#! /bin/sh
set -x -e
export LANG=en_US.UTF-8
MOBSSP_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
source ${MOBSSP_HOME}/conf/mobssp-env.sh
javaopt="-Dlog4j.configuration=file:./log4j.properties"
echo $javaopt
echo "start ........"
flink run  -c com.flink.learn.sql.stream.entry.FlinkLearnStreamDDLSQLEntry \
 $stream_jar $profilePath
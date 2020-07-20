#!/bin/sh

# 初始化环境
set -x
export MOBSSP_ENV=${mobssp.env}
export mobssp_shell_mock=0
export MOBSSP_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
profilePath=$MOBSSP_HOME/conf/application.properties
stream_jar=$MOBSSP_HOME/lib/ssp-stream-${project.version}.jar
sample_jar=$MOBSSP_HOME/lib/ssp-sample-${project.version}.jar

log_path=$MOBSSP_HOME/conf/log4j.properties


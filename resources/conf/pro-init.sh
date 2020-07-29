#!/bin/sh

# 初始化环境
set -x
export PRO_ENV=${pro.env}
export PRO_shell_mock=0
export PRO_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
profilePath=$PRO_HOME/conf/application.properties
stream_jar=$PRO_HOME/lib/stream-${project.version}.jar
sample_jar=$PRO_HOME/lib/sample-${project.version}.jar

log_path=$PRO_HOME/conf/log4j.properties


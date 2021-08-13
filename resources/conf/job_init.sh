#!/bin/sh
# 初始化环境
set -x
export PRO_ENV=${flinklearn.env}
export PRO_HOME="$(cd "`dirname "$0"`"/../../..; pwd)"
profilePath=$PRO_HOME/conf/application.properties
ddl_sql_jar=$PRO_HOME/lib/flink-sql-${project.version}.jar
datalake_jar=$PRO_HOME/lib/flink-datalak-${project.version}.jar
core_jar=$PRO_HOME/lib/flink-core-${project.version}.jar
log_path=$PRO_HOME/conf/log4j.properties



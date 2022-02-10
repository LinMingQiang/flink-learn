FROM openjdk:8-jre-alpine
RUN echo "hello docker"
RUN apk add --no-cache bash
#RUN apk add --no-cache bash snappy libc6-compat
#问题1： sink为 socket的时候无法初始化，外部访问不了容器的端口？

#暴露端口
EXPOSE 8083 9813
# 这个路径只能是相对路径不能是绝对路径
ADD ./flink-1.13.2 /opt/flink-1.13.2
ADD ./build-target /opt/sql-gateway
ADD ./hive-3.1.2 /opt/hive-3.1.2
ENV FLINK_HOME "/opt/flink-1.13.2"
ENV FLINK_CONF_DIR "/opt/flink-1.13.2/conf"
ENV HIVE_CONF "/opt/hive-3.1.2"
ENV HADOOP_CONF_DIR "/opt/hive-3.1.2"
ENV YARN_CONF_DIR "/opt/hive-3.1.2"
CMD ["/opt/sql-gateway/bin/sql-gateway.sh"]
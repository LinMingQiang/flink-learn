# flink-demo 
flink 1.8.0
kafka 0.10
整体流程示意

kafka ->  flink (state管理) -> hbase

由于flink 带有的checkpoint ，所以可以实现 准确的一次性统计

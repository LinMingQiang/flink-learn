# Flink Learn
---
|   Flink                 | scala version      |Kafka version   |
|:------------------:|:------------------:|:------------------:|
| **1.14.2**    | **2.12+**             | **0.10+**             |
---
```
Maven 模板 : https://www.mvnjar.com/org.apache.flink/flink-quickstart-scala/jar.html <br>
注意 ： 当在idea上调试时，先将 es-shade install。然后 ignore Project：es-shade ；否则有冲突问题
```
**Flink Submit**
```
！：1.10之后去掉 -yn ，加上会报错
1：单独资源： flink run -m yarn-cluster --yarnname _wc -p 5 -yjm 1024 -ytm 1024 -ys 2 WordCount.jar
2：共享空间： yarn-session.sh -n 5 -jm 1024 -tm 1024 -s 2 // 在yarn提前开辟一个资源空间 application_1567318548013_0001。 <br>
在开辟的空间上提交任务： flink run -m yarn-cluster --yarnname flink_wc -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 -yid application_1567318548013_0001 WordCount.jar <br>
```
**测试的内容包括**
```
Demo和一些注解
1: Flink DDL
2: FLink Connector
3: Flink Calcite
4: Flink Join
5: FLink 维表
6: Flink Window
7: FLink Datalake
8: Flink Hive
9: Flink CDC
10: FLink 时态表
11: Flink State
12: Flink CEP
13: Flink StateProcessApi
14: Flink Bug
```
**Flink API** <br>
[官方文档 ： Table DataStream 互转](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/common.html#convert-a-datastream-or-dataset-into-a-table)
```
StreamExecutionEnvironment : 流式相关。不能使用SQL的API。如果要在流里面用SQL，使用下面的
StreamTableEnvironment ： 流式SQL相关。可以使用 SQL的API。如果要用Stream相关的，需要将tableData.toRetractStream[Row]
```
---
```
mvn编译参数加上： -Xmx2g -XX:MaxMetaspaceSize=1024m
window 系统在 assembly.xml 里面使用 <outputDirectory>./</outputDirectory>
否则报错：
OS=Windows and the assembly descriptor contains a *nix-specific root-relative-reference (starting with slash)
```
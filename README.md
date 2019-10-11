# flink-demo 
flink 1.8.0 <br>
kafka 0.10 <br>
整体流程示意 <br>
kafka ->  flink (state管理) -> hbase(redis) <br>

idea 创建 maven 模板 <br>
https://www.mvnjar.com/org.apache.flink/flink-quickstart-scala/jar.html <br>

---------------
flink 提交 <br>
>- ./yarn-session.sh -n 5 -jm 1024 -tm 1024 -s 2 // 在yarn提前开辟一个资源空间。 <br>
flink run ../examples/batch/WordCount.jar <br>
  或者
flink run -m yarn-cluster --yarnname flink_word_count -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 -yid application_1567318548013_186353 ../examples/batch/WordCount.jar <br>
>- ./flink run -m yarn-cluster --yarnname flink_word_count -p 5 -yn 4 -yjm 1024 -ytm 1024 -ys 2 ../examples/batch/WordCount.jar <br>
----------------

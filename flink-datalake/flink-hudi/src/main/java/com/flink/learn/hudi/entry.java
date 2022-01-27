package com.flink.learn.hudi;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import scala.Tuple2;

import java.util.concurrent.ExecutionException;

public class entry {
	public static StreamTableEnvironment tableEnv = null;
	public static StreamExecutionEnvironment env = null;
	public static String path = "hdfs://localhost:9000/tmp/hudi/hudi_test_msg_cnt";
	public static String name = "hive";
	public static String defaultDatabase = "test";
	public static String hiveConfDir = "/Users/eminem/programe/hadoop/hive-3.1.2/conf";

	/**
	 * https://hudi.apache.org/cn/docs/next/flink-quick-start-guide
	 * hudi 有个个离线 compaction
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		init();
//		createTable("hive.test.hudi_test_msg_cnt");
//		kafkaToHudi();

		readHudi();
	}

	public static void kafkaToHudi() throws ExecutionException, InterruptedException {
		// {"msg":"msg"}
		// insert into hive.test.hudi_test_msg_cnt values('msg', 1);
		String insertSql = "insert into hive.test.hudi_test_msg_cnt select msg,count(1) cnt from hive.test.src_kafka_msg group by msg";
		tableEnv.executeSql(insertSql).await();
	}

	public static void readHudi() throws Exception {
		tableEnv.toRetractStream(tableEnv.sqlQuery("select * from hive.test.hudi_test_msg_cnt"), Row.class)
				.print();
		env.execute();
	}


	public static void createTable(String tableName) {
		String s = "CREATE TABLE " + tableName + "(\n" +
				"    msg STRING PRIMARY KEY NOT ENFORCED,\n" +
				"    cnt BIGINT \n" +
				") PARTITIONED BY (`msg`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'path' = '" + path + "'," +
				"    'read.streaming.enabled' = 'true'," +
				"    'read.streaming.start-commit' = '1643279889754'," + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.streaming.check-interval' = '4'," + // 指定检查新的commit的周期，默认是60秒
				"    'compaction.async.enabled' = 'false'," + // 在线compaction
//				"    'compaction.delta_seconds' = '10'," + // 10s触发一次compaction
				"    'index.bootstrap.enabled' = 'true'," +
				"    'changelog.enabled' = 'true'," +
				"    'write.tasks' = '3'" + // 并行度，其他是跟着 default并行度一起的

		")";
		System.out.println(s);
		tableEnv.executeSql(s);
	}

	/**
	 * 需要设置 ckp，否则数据不commit
	 */
	public static void init() {
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.getCheckpointConfig().setCheckpointInterval(10000L);
		env.setStateBackend(new EmbeddedRocksDBStateBackend());
		EnvironmentSettings sett = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		tableEnv = StreamTableEnvironment.create(env, sett);
		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
		tableEnv.registerCatalog(name, hive);
	}
}

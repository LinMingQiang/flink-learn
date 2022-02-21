package com.flink.learn.hudi;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.hudi.cli.commands.SparkMain;

import java.util.concurrent.ExecutionException;

public class HudiTest {
	public static StreamTableEnvironment tableEnv = null;
	public static StreamExecutionEnvironment env = null;
	public static String path = "hdfs://localhost:9000/tmp/hudi/";
	public static String name = "hive";
	public static String defaultDatabase = "test";
	public static String hiveConfDir = "/Users/eminem/programe/hadoop/hive-3.1.2/conf";

	/**
	 * https://hudi.apache.org/cn/docs/next/flink-quick-start-guide
	 * hudi 有个个离线 compaction
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
//		SparkMain.main(new String[1]);
		init();
//		createTable("hive.test.hudi_test");
		// 在hive同步建表，这样可以通过hive查询数据
//		createHiveSyncTable("hive.test.hudi_svp_test");
		kafkaToHudi("hive.test.hudi_svp_test");


//		createHiveSyncCOWTable("hive.test.hudi_cow_test");
//		kafkaToHudi("hive.test.hudi_cow_test");
//		readHudi("hive.test.hudi_svp_test");
	}

	/**
	 * insert 的顺序要和建表一样
	 * parquet 是包含了全量数据的
	 * @param targetTable
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void kafkaToHudi(String targetTable) throws ExecutionException, InterruptedException {
		// 一条一个 commit。10s中一次
		// {"msg":"1","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"2","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"3","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"4","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"vv","rowtime":"2021-01-01 11:11:11"}  {"msg":"10","rowtime":"2021-01-01 11:11:11"}
// 进行 compaction ,这时候hive查询的是 compaction 1 的文件 parquet
		// 在输入5个
		// {"msg":"1","rowtime":"2021-01-01 12:11:11"}
		// {"msg":"2","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"3","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"4","rowtime":"2021-01-01 11:11:11"}
		// {"msg":"5","rowtime":"2021-01-01 11:11:11"}  {"msg":"10","rowtime":"2021-01-01 11:11:11"}
// 进行 compaction,这时候hive查询的是 compaction 2 的文件 parquet

// 由 flink 设置 start 和 end ，可以查到每个版本的数据
// 所以compaction的每个parquet文件里面 是包含每个commit的数据？

		// insert into hive.test.hudi_test_msg_cnt values('msg', 1);
		// hive.test.hudi_test_msg_cnt
		String insertSql = "insert into " + targetTable +
				" select msg," +
				"count(1) cnt," +
				"max(rowtime) as rowtime," +
				" dt" +
				" from hive.test.kafka_source group by dt, msg";
		tableEnv.executeSql(insertSql).await();
	}

	public static void readHudi(String tbl) throws Exception {
		tableEnv.toRetractStream(tableEnv.sqlQuery("select * from "+ tbl), Row.class)
				.print();
		env.execute();
	}


	/**
	 * write.operator = upsert
	 * hive可以直接查数据
	 * 映射为 hive的两张表，一张是 _rt (增量实时表)  _ro （读优化视图）
	 * parquet是全量数据，然后加上对应时间之后的log文件就是全量的数据
	 * @param tableName
	 */
	public static void createHiveSyncTable(String tableName) {
		String s = "CREATE TABLE " + tableName + "(\n" +
				"    msg STRING PRIMARY KEY NOT ENFORCED,\n" +
				"    cnt BIGINT," +
				"    rowttime TIMESTAMP(3)," +
				"	`dt` STRING\n" +
				") PARTITIONED BY (`dt`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'path' = '" + path + tableName + "'," +
				"    'write.precombine' = 'true'," + // 是否启用预聚合，一个commit里面。按 主键去重，选时间最新的。
				"    'write.bucket_assign.tasks' = '1'," + // 决定最终写出去的文件
				// 读取的时候配置
				"    'read.streaming.enabled' = 'true'," +
				"    'read.streaming.start-commit' = '20220209170729182'," + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.streaming.check-interval' = '4'," + // 指定检查新的commit的周期，默认是60秒
				// mor的配置
				"    'compaction.schedule.enable' = 'true'," + // 生成 compaction计划，（建议开启，可以配合离线compaction使用）
				"    'compaction.async.enabled' = 'true'," + // 在线执行 compaction
				"    'compaction.delta_seconds' = '10'," + // 10s触发一次compaction
				"	 'hive_sync.enable' = 'true'," +
				"	'hive_sync.mode' = 'hms'," +
				"	'hive_sync.metastore.uris' = 'thrift://localhost:9083'," +
				"	'hive_sync.db' = 'test'," +
				"	'hive_sync.table' = 'hudi_test'," +
				"    'index.bootstrap.enabled' = 'true'," +
				"    'changelog.enabled' = 'false'," + // false = upsert, true = full (一条输入会产生 2条到 hudi sink)
				"    'write.tasks' = '2'" + // 并行度，其他是跟着 default并行度一起的
				")";
		System.out.println(s);
		tableEnv.executeSql(s);
	}
	/**
	 * write.operator = upsert
	 * 一个commit 一个 parquet文件，且都是全量的
	 * @param tableName
	 */
	public static void createHiveSyncCOWTable(String tableName) {
		String s = "CREATE TABLE " + tableName + "(\n" +
				"    msg STRING PRIMARY KEY NOT ENFORCED,\n" +
				"    cnt BIGINT," +
				"	`dt` STRING\n" +
				") PARTITIONED BY (`dt`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'COPY_ON_WRITE',\n" +
				"    'path' = '" + path + tableName + "'," +
				// 读取的时候配置
				"    'read.streaming.enabled' = 'true'," +
				"    'read.streaming.start-commit' = '20220209170729182'," + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.streaming.check-interval' = '4'," + // 指定检查新的commit的周期，默认是60秒
				"	 'hive_sync.enable' = 'true'," +
				"	'hive_sync.mode' = 'hms'," +
				"	'hive_sync.metastore.uris' = 'thrift://localhost:9083'," +
				"	'hive_sync.db' = 'test'," +
				"	'hive_sync.table' = 'hudi_test'," +
				"    'index.bootstrap.enabled' = 'true'," +
				"    'changelog.enabled' = 'false'," + // false = upsert, true = full (一条输入会产生 2条到 hudi sink)
				"    'write.tasks' = '3'" + // 并行度，其他是跟着 default并行度一起的
				")";
		System.out.println(s);
		tableEnv.executeSql(s);
	}

	/**
	 * 当我们从  read.start-commit 和  read.end-commit 查询的时候:
	 * 1: 首先输出的是，这段commit 时间段内的最新值
	 * 2：然后再输出一条最新值，因为是流模式读取的，所以肯定会更新到最新的值
	 * @param tableName
	 */
	public static void createTable(String tableName) {
		String s = "CREATE TABLE " + tableName + "(\n" +
				"    msg STRING PRIMARY KEY NOT ENFORCED,\n" +
				"    cnt BIGINT," +
				"	`dt` STRING\n" +
				") PARTITIONED BY (`dt`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'path' = '" + path + tableName + "'," +
				// 读取的时候配置
				"    'read.streaming.enabled' = 'true'," +
				"    'read.start-commit' = '20220209170729182'," + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.end-commit' = '20220209170729182'," + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.streaming.check-interval' = '4'," + // 指定检查新的commit的周期，默认是60秒
				// mor的配置
				"    'compaction.schedule.enable' = 'true'," + // 生成 compaction计划，（建议开启，可以配合离线compaction使用）
				"    'compaction.async.enabled' = 'true'," + // 在线执行 compaction
				"    'compaction.delta_seconds' = '10'," + // 10s触发一次compaction
				"    'index.bootstrap.enabled' = 'true'," +
				"    'changelog.enabled' = 'false'," + // false = upsert, true = full (一条输入会产生 2条到 hudi sink)
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
		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, "3.1.2");
		tableEnv.registerCatalog(name, hive);
	}
}

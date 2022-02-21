package com.flink.learn.hudi;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.hudi.common.model.OverwriteNonDefaultsWithLatestAvroPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;

import java.util.concurrent.ExecutionException;

public class HudiColumnsTest {
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
		init();
//		createHiveSyncTable("hive.test.hudi_col_test");
		kafkaToHudi("hive.test.hudi_col_test");
//		readHudi("hive.test.hudi_col_test");
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
		// {"msg":"1","rowtime":"2021-01-01 11:11:11","age":40000}
		// {"msg":"1","rowtime":"2021-01-01 11:11:11"}
		String insertSql = "insert into " + targetTable +
				" select msg,name,age,count(1) cnt,dt from hive.test.kafka_source group by dt,msg,name,age";
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
				"	`name` STRING,\n" +
				"	`age` int,\n" +
				"    cnt BIGINT," +
				"	`dt` STRING\n" +
				") PARTITIONED BY (`dt`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'path' = '" + path + tableName + "'," +
				"    'write.payload.class' = '"+OverwriteNonDefaultsWithLatestAvroPayload.class.getName()+"',\n" +
				"    'write.precombine' = 'true',\n" + // 是否启用预聚合，一个commit里面。按 主键去重，选时间最新的。
				// 读取的时候配置
				"    'read.streaming.enabled' = 'true',\n" +
				"    'read.streaming.start-commit' = '20220209170729182',\n" + // read.streaming.start-commit 时间戳后的所有数据。该功能的特殊在于可以同时在流和批的 pipeline 上执行。
				"    'read.streaming.check-interval' = '4',\n" + // 指定检查新的commit的周期，默认是60秒
				// mor的配置
				"    'compaction.schedule.enable' = 'true',\n" + // 生成 compaction计划，（建议开启，可以配合离线compaction使用）
				"    'compaction.async.enabled' = 'true',\n" + // 在线执行 compaction
				"    'compaction.delta_seconds' = '10',\n" + // 10s触发一次compaction
				"	 'hive_sync.enable' = 'true',\n" +
				"	'hive_sync.mode' = 'hms'," +
				"	'hive_sync.metastore.uris' = 'thrift://localhost:9083',\n" +
				"	'hive_sync.db' = 'test',\n" +
				"	'hive_sync.table' = 'hudi_col_test',\n" +
				"    'index.bootstrap.enabled' = 'true',\n" +
				"    'changelog.enabled' = 'false',\n" + // false = upsert, true = full (一条输入会产生 2条到 hudi sink)
				"    'write.tasks' = '3'" + // 并行度，其他是跟着 default并行度一起的
				")";
		System.out.println(s);
//		tableEnv.executeSql(s);
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

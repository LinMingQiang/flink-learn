package com.flink.learn.catalog.entry;

import com.sql.manager.DDLSourceSQLManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.concurrent.ExecutionException;

public class HiveReadEntry {
	public static StreamTableEnvironment tableEnv = null;
	public static String name = "hcatalog";
	public static String defaultDatabase = "test";
	public static String hiveConfDir = "/Users/eminem/programe/hadoop/hive-3.1.2/conf";

	public static void main(String[] args) throws Exception {
		init();
//		createTableToHive();
		dropTableToHive();
//		selectTable();
		// 在hive终端 通过 show create table flink_kafka_test 可以看到建表信息，跟正常的hive表不一样
		// 必须转回 default_catalog，否则找不到sink表。
//        tableEnv.useCatalog("default_catalog");
		//tableEnv.createTemporaryView("hive_result", r);
	}


	public static void createTableToHive() {
		tableEnv.useCatalog(name);
		String sql = DDLSourceSQLManager.createStreamFromKafka("localhost:9092", "test", "flink_kafka_test", "test", "json");
		tableEnv.executeSql(sql);
		for (String s : tableEnv.listTables()) {
			System.out.println(s);
		}
	}

	public static void dropTableToHive() {
		tableEnv.useCatalog(name);
		for (String s : tableEnv.listTables()) {
			System.out.println(s);
			tableEnv.executeSql("drop table test.flink_kafka_test" );
		}
	}
	/**
	 * {"rowtime":"2020-01-01 00:00:01","msg":"c"}    .
	 * @throws Exception
	 */
	public static void selectTable() throws Exception {
		tableEnv.executeSql(DDLSourceSQLManager.createPrintSinkTbl("printTbl"));
		// 不设置catalaog就能读取hive的表
		StatementSet set = tableEnv.createStatementSet();
		set.addInsert("printTbl", tableEnv.sqlQuery("select msg, proctime, rowtime from hcatalog.test.flink_kafka_test"));
		set.execute().await();
	}


	public static void init() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings sett = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		tableEnv = StreamTableEnvironment.create(env, sett);
		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
		tableEnv.registerCatalog(name, hive);
	}
}

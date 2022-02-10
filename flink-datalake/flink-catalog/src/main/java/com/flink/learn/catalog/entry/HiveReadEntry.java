package com.flink.learn.catalog.entry;

import com.sql.manager.DDLSourceSQLManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.concurrent.ExecutionException;

public class HiveReadEntry {
	public static StreamTableEnvironment tableEnv = null;
	public static String name = "hcatalog";
	public static String defaultDatabase = "test";
	public static String hiveConfDir = "/Users/eminem/programe/hadoop/hive-3.1.2/conf";

	public static void main(String[] args) throws Exception {
		init();
		createTableToHive();
//		dropTableToHive();
//		selectTable();
//		descTable();
		// 在hive终端 通过 show create table flink_kafka_test 可以看到建表信息，跟正常的hive表不一样
		// 必须转回 default_catalog，否则找不到sink表。
//        tableEnv.useCatalog("default_catalog");
		//tableEnv.createTemporaryView("hive_result", r);
	}

	public static void descTable() throws TableNotExistException {
//		String sql = DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
//				"test",
//				"flink_kafka_test",
//				"test",
//				"json");
//		tableEnv.executeSql(sql);

//		tableEnv
//				.getCatalog("default_catalog")
//				.get()
//				.getTable(ObjectPath.fromString("default_database.flink_kafka_test"))
//				.getOptions()
//				.forEach((x, v) -> System.out.println(x + ": " + v));
//		CatalogBaseTable tbl = tableEnv
//				.getCatalog("hcatalog")
//				.get()
//				.getTable(ObjectPath.fromString("test.test"));

		// 不需要 flink env
		HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
		hive.open();
		CatalogBaseTable tbl = hive.getTable(ObjectPath.fromString("test.test"));
		tbl.getUnresolvedSchema().getColumns().forEach(x -> {
			System.out.println(x);
		});
				tbl
				.getOptions()
				.forEach((x, v) -> System.out.println(x + ": " + v));
		hive.close();
		//		System.out.println();
//		String sql = "show create table hcatalog.test.flink_kafka_test";
//		tableEnv.executeSql(sql);
	}


	public static void createTableToHive() {
		// show create table flink_kafka_test;
		String sql = DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
				"test",
				"hcatalog.test.kafka_source",
				"test",
				"json");
		tableEnv.executeSql(sql);
		for (String s : tableEnv.listTables()) {
			System.out.println(s);
		}
	}

	public static void dropTableToHive() {
		for (String s : tableEnv.listTables()) {
			tableEnv.executeSql("drop table hcatalog.test.flink_kafka_test");
		}
	}

	/**
	 * {"rowtime":"2020-01-01 00:00:01","msg":"c"}    .
	 *
	 * @throws Exception
	 */
	public static void selectTable() throws Exception {
		tableEnv.executeSql(DDLSourceSQLManager.createPrintSinkTbl("printTbl"));
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

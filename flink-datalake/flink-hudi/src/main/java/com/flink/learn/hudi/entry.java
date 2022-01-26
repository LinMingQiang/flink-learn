package com.flink.learn.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class entry {
	public static StreamTableEnvironment tableEnv = null;
	public static String path = "hdfs://localhost:9000/tmp";

	public static void main(String[] args) {
		init();
		String s = "CREATE TABLE hudi_users5(\n" +
				"    msg STRING PRIMARY KEY NOT ENFORCED,\n" +
				"    cnt BIGINT \n" +
				") PARTITIONED BY (`partition`) WITH (\n" +
				"    'connector' = 'hudi',\n" +
				"    'table.type' = 'MERGE_ON_READ',\n" +
				"    'path' = '" + path + "'" +
				")";
		tableEnv.executeSql(s);

	}


	public static void init() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings sett = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		tableEnv = StreamTableEnvironment.create(env, sett);
	}
}

package com.flink.learn.offline.entry;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveReadEntry {
    public static void main(String[] args) {
        String sql = args[0];
        System.out.println(sql);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name = "hcatalog";
        String defaultDatabase = "test";
        String hiveConfDir = "/Users/eminem/programe/hadoop/hive-3.1.2/conf";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hive);
        for (String s : tableEnv.listCatalogs()) {
            System.out.println(s);
        }

        // 必须转hive才能找到hive的表
        tableEnv.useCatalog(name);



        // 必须转回 default_catalog，否则找不到sink表。
        tableEnv.useCatalog("default_catalog");
        //tableEnv.createTemporaryView("hive_result", r);
    }
}

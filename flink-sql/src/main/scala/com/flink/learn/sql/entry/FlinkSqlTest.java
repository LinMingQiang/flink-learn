package com.flink.learn.sql.entry;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkSqlTest {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tbl = TableEnvironment.create(settings);
    }
}

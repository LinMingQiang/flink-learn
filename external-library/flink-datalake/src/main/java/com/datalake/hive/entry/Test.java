package com.datalake.hive.entry;

import com.core.FlinkEvnBuilder;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Duration;

public class Test {
    public static void main(String[] args) throws Exception {
        String proPath = args[0];
        StreamExecutionEnvironment streamEnv = null;
        FlinkLearnPropertiesUtil.init(
                proPath,
                "Flink_Hive_Test");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(
                FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv, Duration.ofHours(1L));

        HiveCatalog hive = new HiveCatalog("myhive", "dw_adplatform_log_test", "/etc/hive/conf");
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        String insertSQL = "select * from flink_kafka";
        tableEnv.toAppendStream(tableEnv.sqlQuery(insertSQL), Row.class).print();
        streamEnv.execute();

    }
}

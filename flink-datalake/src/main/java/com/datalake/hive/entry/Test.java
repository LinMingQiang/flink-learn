package com.datalake.hive.entry;


import com.core.FlinkEvnBuilder;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

/**
 * 'sink.partition-commit.trigger'='process-time',
 * -- 只要触发checkpoint就commit，partition暂时不可用（因为watermark不生成）
 * execution.checkpointing.interval: 100000 -- ckp触发间隔配置
 * state.checkpoints.dir: hdfs://ShareSdkHadoop/tmp/flink/checkpoint
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String p = "/application.properties";
        if(args.length > 0) {
            p = args[0];
        }
        FlinkLearnPropertiesUtil.init(
                p,
                "Flink_Hive_Test");
//        StreamExecutionEnvironment streamEnv = null;
//        streamEnv = FlinkEvnBuilder.buildStreamingEnv(
//                FlinkLearnPropertiesUtil.param(),
//                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
//                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings sett =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(executionEnvironment);
//        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
//                streamEnv, Duration.ofHours(1L));

        HiveCatalog hive = new HiveCatalog(
                "myhive",
                "dw_adplatform_log_test",
                "/etc/hive/conf");
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        String insertSQL = "select * from flink_kafka";
        Table t = tableEnv.sqlQuery(insertSQL);
        DataSet<Row> result = tableEnv.toDataSet(t, Row.class);

//        tableEnv.toAppendStream(tableEnv.sqlQuery(insertSQL), Row.class).print();
//        streamEnv.executeAsync();
        tableEnv.executeSql("INSERT INTO hive_table SELECT app_id, adslot_id, sys_time, DATE_FORMAT(rt, 'yyyy-MM-dd') as dt, DATE_FORMAT(rt, 'HH') as hr" +
                "  FROM flink_kafka");


    }
}

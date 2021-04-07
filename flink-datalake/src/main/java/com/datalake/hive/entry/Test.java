package com.datalake.hive.entry;


import com.core.FlinkEvnBuilder;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

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
        StreamExecutionEnvironment streamEnv = null;
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(
                FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv, Duration.ofHours(1L));

        HiveCatalog hive = new HiveCatalog(
                "myhive",
                "dw_adplatform_log_test",
                "/etc/hive/conf");
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
//        String insertSQL = "select * from flink_kafka";
//        tableEnv.toAppendStream(tableEnv.sqlQuery(insertSQL), Row.class).print();
//        streamEnv.executeAsync();
        tableEnv.executeSql("INSERT INTO hive_table SELECT app_id, adslot_id, sys_time, DATE_FORMAT(rt, 'yyyy-MM-dd') as dt, DATE_FORMAT(rt, 'HH') as hr" +
                "  FROM flink_kafka");


    }
}

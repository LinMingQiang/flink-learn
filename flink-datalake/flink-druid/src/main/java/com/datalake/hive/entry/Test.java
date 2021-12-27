package com.datalake.hive.entry;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 'sink.partition-commit.trigger'='process-time',
 * -- 只要触发checkpoint就commit，partition暂时不可用（因为watermark不生成）
 * execution.checkpointing.interval: 100000 -- ckp触发间隔配置
 * state.checkpoints.dir: hdfs://ShareSdkHadoop/tmp/flink/checkpoint
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String p = "/application.properties";
        if (args.length > 0) {
            p = args[0];
        }
        FlinkLearnPropertiesUtil.init(
                p,
                "Flink_Hive_Test");
        // 设置运行模式
        EnvironmentSettings sett =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //          StreamTableEnvironment是不支持 batch模式的，但是他支持读取有边界的数据流
        //        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //          StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv, Duration.ofHours(1L));

        // TableEnvironment是没有datastream的api的，所有Batch模式只能用sql，
        TableEnvironment tableEnv = TableEnvironment.create(sett);
        HiveCatalog hive = new HiveCatalog(
                "myhive",
                "dw_adplatform_log_test",
                "/etc/hive/conf");
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        String insertSQL = "select * from flink_kafka";
        Table t = tableEnv.sqlQuery(insertSQL);
 // 过期
//        DataSet<Row> result = tableEnv.toDataSet(t, Row.class);
        tableEnv.executeSql("INSERT INTO hive_table SELECT app_id, adslot_id, sys_time, DATE_FORMAT(rt, 'yyyy-MM-dd') as dt, DATE_FORMAT(rt, 'HH') as hr" +
                "  FROM flink_kafka");


    }
}

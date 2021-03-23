package com.flink.streamsql.entry;

import com.core.FlinkEvnBuilder;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.Duration;

public class FlinkSQLFactoryEntry {
    public static StreamTableEnvironment tableEnv = null;
// {"rowtime":"2021-01-20 00:00:23","msg":"hello2"}
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment streamEnv = null;
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv, Duration.ofHours(1L));
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        System.out.println(DDLSourceSQLManager.createCustomESSink("essinktest"));
        tableEnv.executeSql(DDLSourceSQLManager.createCustomESSink("essinktest"));
        tableEnv.executeSql("insert into essinktest" +
                " select " +
                "'id' as id," +
                "msg," +
                "count(1) uv " +
                "from test" +
                " group by" +
                " msg");

    }
}

package com.flink.sql.entry;

import com.flink.sql.env.FlinkEvnBuilder;
import com.flink.sql.util.FlinkLearnPropertiesUtil;

import java.io.IOException;
import java.time.Duration;

public class FlinkSqlPlatEntry {
    public static void main(String[] args) throws IOException {
        FlinkLearnPropertiesUtil.init("/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties", "FlinkSqlPlatEntry");
        FlinkEvnBuilder.initEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                0L,
                Duration.ofHours(1L));



    }
}

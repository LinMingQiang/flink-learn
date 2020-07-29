package com.flink.pro.java.entry;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.java.core.FlinkEvnBuilder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;

public class FlinkStreamJoinHbaseEntry {

    public static void main(String[] args) throws IOException {
        FlinkLearnPropertiesUtil.init(args[0],
                "FlinkStreamJoinHbaseEntry");
        StreamTableEnvironment env = FlinkEvnBuilder.buildStreamTableEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL(),
                Time.minutes(10),
                Time.minutes(60));



    }
}

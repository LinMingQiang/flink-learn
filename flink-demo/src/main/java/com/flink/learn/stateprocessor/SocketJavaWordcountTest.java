package com.flink.learn.stateprocessor;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkEvnBuilder;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketJavaWordcountTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "SocketJavaPoJoWordcountTest");
        DataStream source = env.socketTextStream("localhost", 9877);


    }
}

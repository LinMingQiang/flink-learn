package com.core;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.flink.common.kafka.KafkaManager;

public abstract class FlinkStreamEnvAndSource {
    public static ExecutionEnvironment bEnv = null;
    public static StreamExecutionEnvironment streamEnv = null;
    public static StreamTableEnvironment tableEnv = null;

    public static KeyedStream<KafkaManager.KafkaMessge, String> kafkaDataSource = null;
    public static Table kafkaDataTable = null;
}

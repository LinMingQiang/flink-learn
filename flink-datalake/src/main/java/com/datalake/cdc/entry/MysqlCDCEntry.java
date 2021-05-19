package com.datalake.cdc.entry;

import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.core.FlinkEvnBuilder;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Duration;

public class MysqlCDCEntry {
    public static void main(String[] args) throws Exception {
        ddl();
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("localhost")
//                .port(3306)
//                .databaseList("TEST") // monitor all tables under inventory database
//                .username("root")
//                .password("123456")
////                .tableList("TEST.order_info")
//                .serverId(123456)
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env
//                .addSource(sourceFunction)
//                .print()
//                .setParallelism(1); // use parallelism 1 for sink to keep message ordering
//
//        env.execute();
    }


    public static void ddl() throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        StreamExecutionEnvironment streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv,
                Duration.ofHours(2));
        String s = "CREATE TABLE order_info (\n" +
                "  orderCode STRING,\n" +
                "  serviceCode STRING,\n" +
                "  accountPeriod STRING,\n" +
                "  subjectName STRING ,\n" +
                "  subjectCode STRING,\n" +
                "  occurDate TIMESTAMP,\n" +
                "  amt  DECIMAL(11, 2),\n" +
                "  status STRING,\n" +
                "  proc_time AS PROCTIME()  -–使用维表时需要指定该字段\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'TEST',\n" +
                "  'table-name' = 'order_info'\n" +
                ")";
        tableEnv.executeSql(s);

        tableEnv.toAppendStream(tableEnv.from("order_info"), Row.class)
                .print();

        streamEnv.execute();

    }
}

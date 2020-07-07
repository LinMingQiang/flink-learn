package com.streamtable.test;

import com.flink.learn.sql.common.TableSinkManager;
import com.flink.learn.test.common.FlinkStreamTableTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.Description;

public class FlinkLearnStreamExcutionEntry extends FlinkStreamTableTestBase {

    @Test
    public void testWordCount() throws Exception {
        Table a= tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
        ,"topic,offset,msg");
        a.printSchema();
        // sink1
        tableEnv.toAppendStream(a, Row.class).print();
        // sink2
        // TableSinkManager.registAppendStreamTableSink(tableEnv);
        // a.insertInto("test");
       tableEnv.execute("");
    }
    //    test("wordCount") {
//        env
//                .addSource(kafkaSource(TEST_TOPIC, BROKER))
//                .flatMap(_.msg.split("\\|", -1))
//                .map(x => (x, 1))
//      .keyBy(0)
//                .flatMap(new WordCountRichFunction)
//                .print
//        env.execute("lmq-flink-demo") //程序名
//    }

}

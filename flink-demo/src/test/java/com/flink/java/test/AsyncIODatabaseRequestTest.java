package com.flink.java.test;

import com.flink.learn.richf.AsyncIODatabaseRequest;
import com.flink.learn.test.common.FlinkStreamTableTestBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.junit.Test;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg;
import java.util.concurrent.TimeUnit;

public class AsyncIODatabaseRequestTest extends FlinkStreamTableTestBase {
    /**
     * 异步io测试
     */
    @Test
    public void testAsyncIo() throws Exception {
        DataStreamSource<KafkaTopicOffsetMsg> stream = streamEnv
                .addSource(getKafkaSource("test", "localhost:9092", "latest"));
        AsyncDataStream.unorderedWait(
                stream,
                new AsyncIODatabaseRequest(), 10000, TimeUnit.MILLISECONDS, 1)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}

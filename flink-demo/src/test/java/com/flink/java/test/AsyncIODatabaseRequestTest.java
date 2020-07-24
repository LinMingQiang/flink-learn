package com.flink.java.test;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.Test;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg;
public class AsyncIODatabaseRequestTest extends FlinkJavaStreamTableTestBase {
    /**
     * 异步io测试
     */
    @Test
    public void testAsyncIo() throws Exception {
        DataStreamSource<KafkaTopicOffsetMsg> stream = streamEnv
                .addSource(getKafkaSource("test", "localhost:9092", "latest"));
//        AsyncDataStream.unorderedWait(
//                stream,
//                new AsyncIODatabaseRequest(), 10000, TimeUnit.MILLISECONDS, 1)
//                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}

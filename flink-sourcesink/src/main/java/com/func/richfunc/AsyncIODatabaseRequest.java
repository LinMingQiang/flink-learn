package com.func.richfunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.dbutil.FlinkHbaseFactory;
import com.flink.common.kafka.KafkaManager.KafkaMessge;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncIODatabaseRequest extends RichAsyncFunction<KafkaMessge,
        Tuple2<KafkaMessge, KafkaMessge>> {
    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        ParameterTool parame = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(parame);
        FlinkHbaseFactory.getGlobalConn(FlinkLearnPropertiesUtil.ZOOKEEPER());
    }

    @Override
    public void asyncInvoke(KafkaMessge input,
                            ResultFuture<Tuple2<KafkaMessge, KafkaMessge>> resultFuture) throws Exception {
        final Future<KafkaMessge> result = queryClient(input);
        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<KafkaMessge>() {
            @Override
            public KafkaMessge get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept((KafkaMessge dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
        });
    }

    /**
     * 释放过期，否则超过个数导致反压
     *
     * @param input
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void timeout(KafkaMessge input,
                        ResultFuture<Tuple2<KafkaMessge, KafkaMessge>> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(new Tuple2<>(input,
                new KafkaMessge(
                        input.topic(),
                        input.offset(),
                        0L,
                        "timeout",
                        null,
                        null))));
    }

    public Future<KafkaMessge> queryClient(KafkaMessge input) {
        return new Future<KafkaMessge>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public KafkaMessge get() {
                if (input.msg().equals("1")) {
                    // FlinkHbaseFactory.get(conn, "test", input.msg());

                    return new KafkaMessge(
                            input.topic(),
                            input.offset(),
                            0L,
                            "hello",
                            null,
                            null);
                } else {
                    return input;
                }
            }

            @Override
            public KafkaMessge get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return get();
            }
        };
    }
}

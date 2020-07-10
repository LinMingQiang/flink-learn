package com.flink.learn.richf;

import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncIODatabaseRequest extends RichAsyncFunction<KafkaManager.KafkaTopicOffsetMsg,
        Tuple2<KafkaManager.KafkaTopicOffsetMsg, KafkaManager.KafkaTopicOffsetMsg>> {

    @Override
    public void asyncInvoke(KafkaManager.KafkaTopicOffsetMsg input,
                            ResultFuture<Tuple2<KafkaManager.KafkaTopicOffsetMsg, KafkaManager.KafkaTopicOffsetMsg>> resultFuture) throws Exception {
        final Future<KafkaManager.KafkaTopicOffsetMsg> result = queryClient(input);
        // 设置客户端完成请求后要执行的回调函数
        // 回调函数只是简单地把结果发给 future
        CompletableFuture.supplyAsync(new Supplier<KafkaManager.KafkaTopicOffsetMsg>() {
            @Override
            public KafkaManager.KafkaTopicOffsetMsg get() {
                try {
                    return result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // 显示地处理异常。
                    return null;
                }
            }
        }).thenAccept( (KafkaManager.KafkaTopicOffsetMsg dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
        });
    }

    @Override
    public void timeout(KafkaManager.KafkaTopicOffsetMsg input,
                        ResultFuture<Tuple2<KafkaManager.KafkaTopicOffsetMsg, KafkaManager.KafkaTopicOffsetMsg>> resultFuture) throws Exception {

    }

    public Future<KafkaManager.KafkaTopicOffsetMsg> queryClient(KafkaManager.KafkaTopicOffsetMsg input) {
        return new Future<KafkaManager.KafkaTopicOffsetMsg>() {

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
            public KafkaManager.KafkaTopicOffsetMsg get() throws InterruptedException, ExecutionException {
                Thread.sleep(1000L * Long.valueOf(input.msg()));
                return input;
            }

            @Override
            public KafkaManager.KafkaTopicOffsetMsg get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                Thread.sleep(1000L * Long.valueOf(input.msg()));
                return input;
            }
        };
    }
}

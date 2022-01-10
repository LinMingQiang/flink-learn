package com.flink.state.processor.api.trans;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

import com.flink.state.processor.api.pojo.WordCountGroupByKey;
import com.flink.state.processor.api.pojo.WordCountPoJo;

public class AccountJavaPojoKeyedStateBootstrapFunction
        extends KeyedStateBootstrapFunction<WordCountGroupByKey, WordCountPoJo> {
    ValueState<WordCountPoJo> lastState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<WordCountPoJo> descriptor =
                new ValueStateDescriptor("wordcountState", WordCountPoJo.class);
        lastState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(WordCountPoJo value, Context ctx) throws Exception {
        lastState.update(
                new WordCountPoJo(
                        value.word,
                        1000L + value.count,
                        value.timestamp,
                        value.srcArr,
                        value.keyby));
    }
}

package com.flink.learn.trans;

import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.bean.WordCountPoJo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class AccountJavaKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<String, TranWordCountPoJo> {
   // ValueState<TranWordCountPoJo> lastState;
   ValueState<WordCountPoJo> lastState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<WordCountPoJo> descriptor =
                new ValueStateDescriptor("wordcountState", WordCountPoJo.class);
        lastState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TranWordCountPoJo value, Context ctx) throws Exception {
        value.count = 10L;
        WordCountPoJo w = new WordCountPoJo();
        w.word = value.word;
        w.count = 1000L;
        w.timestamp = value.timestamp;
        lastState.update(w);
    }
}

package com.flink.learn.trans;

import com.flink.learn.bean.CaseClassUtil;
import com.flink.learn.bean.TranWordCountPoJo;
import com.flink.learn.bean.WordCountPoJo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class AccountJavaKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<String, TranWordCountPoJo> {
    ValueState<TranWordCountPoJo> lastState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<TranWordCountPoJo> descriptor =
                new ValueStateDescriptor<TranWordCountPoJo>("wordcountState", TranWordCountPoJo.class);
        lastState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TranWordCountPoJo value, Context ctx) throws Exception {
        value.count = 10L;
        lastState.update(value);
    }
}

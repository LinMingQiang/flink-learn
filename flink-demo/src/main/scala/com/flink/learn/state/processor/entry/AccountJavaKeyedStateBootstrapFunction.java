package com.flink.learn.state.processor.entry;

import com.flink.learn.bean.CaseClassUtil.Wordcount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.TypeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;

public class AccountJavaKeyedStateBootstrapFunction extends KeyedStateBootstrapFunction<String, Wordcount> {
    ValueState<Wordcount> lastState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Wordcount> descriptor =
                new ValueStateDescriptor<Wordcount>("wordcountState", Wordcount.class);
        lastState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(Wordcount value, Context ctx) throws Exception {
        lastState.update(value);
    }
}

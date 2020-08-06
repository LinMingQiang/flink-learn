package com.flink.learn.trans;

import com.flink.learn.bean.WordCountPoJo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

public class AcountJavaPoJoOperatorStateBootstrap extends StateBootstrapFunction<WordCountPoJo> {
    ListState<WordCountPoJo> checkpointedState = null ; // checkpoint state
    @Override
    public void processElement(WordCountPoJo value, Context ctx) throws Exception {
        value.setCount(2000L + value.count);
        checkpointedState.add(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor<WordCountPoJo>(
                "opearatorstate",
                WordCountPoJo.class);
        checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
    }
}

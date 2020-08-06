package com.flink.learn.sink;

import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.bean.WordCountPoJo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WordCountJavaSink extends RichSinkFunction<WordCountPoJo> implements CheckpointedFunction {
    ListState<WordCountPoJo> checkpointedState = null ; // checkpoint state
    HashMap<WordCountGroupByKey, WordCountPoJo> buffer = new HashMap<WordCountGroupByKey, WordCountPoJo>();
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for(Map.Entry<WordCountGroupByKey, WordCountPoJo> tmp : buffer.entrySet()){
            checkpointedState.add(tmp.getValue());
            System.out.println(">> " + tmp);
        }
        System.out.println(">> snapshotState << " + buffer.size());
        buffer.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor<WordCountPoJo>(
                "opearatorstate",
                WordCountPoJo.class);
        checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
        if (context.isRestored()) {
            System.out.println("${taskIndex}> --- initializeState ---");
        }
    }

    @Override
    public void invoke(WordCountPoJo value, Context context) throws Exception {
       // System.out.println(value);
        buffer.put(value.getKeyby(), value);
    }
}

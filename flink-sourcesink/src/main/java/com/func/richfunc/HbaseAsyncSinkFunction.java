package com.func.richfunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import java.util.HashMap;
import java.util.Map;

public class HbaseAsyncSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>>
        implements CheckpointedFunction {
    private int flushSize = 1000;
    private ListState<Row> checkpointedState = null;// checkpoint state
    private HashMap<String, Row> bufferedElements = new HashMap(); // buffer List

    public HbaseAsyncSinkFunction(int flushSize) {
        this.flushSize = flushSize;
    }

    public boolean isTimeToFlush() {
        return bufferedElements.size() >= flushSize;
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        if (value.f0) {
            String tp = (String) value.f1.getField(0);
            long c = (long) value.f1.getField(1);
            bufferedElements.put(tp, value.f1);
            if (isTimeToFlush()) {
                commitFlush();
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(p);
        // init hbase conn
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Map.Entry<String, Row> r : bufferedElements.entrySet()) {
            checkpointedState.add(r.getValue());
        }
        commitFlush();
    }

    public void commitFlush(){
        for (Map.Entry<String, Row> r : bufferedElements.entrySet()) {
            System.out.println("sink : " + r);
        }
        bufferedElements.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor<Row>(
                "hbase-sink-ckp",
                TypeInformation.of(Row.class)
        );
        checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
        if (context.isRestored()) {
            for (Row element : checkpointedState.get()) {
                bufferedElements.put(element.getField(0).toString(), element);
            }
        }

    }
}

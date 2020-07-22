package com.flink.common.java.sinkfunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

public class HbaseAsyncSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>>
implements CheckpointedFunction {

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        System.out.println("sink : " + value);

    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool)getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(p);
        System.out.println(FlinkLearnPropertiesUtil.param());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}

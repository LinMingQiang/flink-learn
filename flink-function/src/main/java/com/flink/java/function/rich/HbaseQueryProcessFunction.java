package com.flink.java.function.rich;

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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class HbaseQueryProcessFunction<IN, OUT> extends KeyedProcessFunction<String, IN, OUT>
        implements CheckpointedFunction {
    private int flushSize = 10;
    private int msgNum = 0;
    private ListState<Row> checkpointedState = null;// checkpoint state
    private HashMap<String, IN> bufferedElements = new HashMap(); // buffer List
    private List<OUT> queryResBuffer = new ArrayList<>(); // ckp的时候结果
    private HbaseQueryFunction<IN, OUT> qf = null;
    private TypeInformation<IN> inType = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(p);
    }

    public HbaseQueryProcessFunction(HbaseQueryFunction qf, TypeInformation<IN> inType) {
        this.qf = qf;
        this.inType = inType;
    }

    // 查询hbase
    public List<IN> queryHbase(HashMap<String, IN> bufferedElements) {
        return new ArrayList<>(bufferedElements.values());
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        if (!bufferedElements.isEmpty()) {
            List<IN> res = queryHbase(bufferedElements);
            for (Object o : qf.transResult(res)) {
                queryResBuffer.add((OUT) o);
            }
            bufferedElements.clear();
        }
        checkpointedState.clear();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor(
                "HbaseQueryRichMapFunction",
                inType
        );
        checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
    }

    /**
     * 处理每个元素
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        // 每个元素在最后一次ckp之后10s后执行一次
        ctx.timerService().registerProcessingTimeTimer(
                new Date().getTime() + FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL()+ 1000L);
        if (!queryResBuffer.isEmpty()) {
            queryResBuffer.forEach(x -> out.collect(x));
            queryResBuffer.clear();
        }
        bufferedElements.put(ctx.getCurrentKey(), value);
        msgNum++;
        if (msgNum >= flushSize) {
            // query hbase
            List<IN> res = queryHbase(bufferedElements);
            for (Object o : qf.transResult(res)) {
                out.collect((OUT) o);
            }
            bufferedElements.clear();
            msgNum = 0;
        }
    }

    /**
     * 当没有数据之后，触发最后一个查询结果.
     * 如果ckp卡主，这里也会延后，所以不会有gap问题
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        // 如果check有剩余数据，就输出，
        if (!queryResBuffer.isEmpty()) {
            queryResBuffer.forEach(x -> out.collect(x));
            queryResBuffer.clear();
        }
    }
}

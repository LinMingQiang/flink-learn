package com.flink.java.function.process;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.java.function.common.util.AbstractHbaseQueryFunction;
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
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @param <IN>  Tuple2<String, KafkaTopicOffsetMsgPoJo>
 * @param <OUT> WordCountPoJo
 */
public class HbaseQueryProcessFunction<IN, OUT> extends KeyedProcessFunction<String, Tuple2<String, IN>, OUT>
        implements CheckpointedFunction {
    private int flushSize = 100;
    private ListState<Tuple2<String, IN>> checkpointedState = null;// checkpoint state
    private List<Tuple2<String, IN>> bufferedElements = new ArrayList<Tuple2<String, IN>>(); // buffer List
    private List<OUT> queryResBuffer = new ArrayList<>(); // ckp的时候结果
    private AbstractHbaseQueryFunction<IN, OUT> qf = null;
    private TypeInformation<Tuple2<String, IN>> inType = null;
    private Table t = null;
    private String tablename = null;

    /**
     * 初始化，初始化hbase等
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool p = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
        FlinkLearnPropertiesUtil.init(p);
        // init hbase conn and table
    }

    public HbaseQueryProcessFunction(
            AbstractHbaseQueryFunction qf,
            String tablename,
            int flushSize,
            TypeInformation<Tuple2<String, IN>> inType) {
        this.qf = qf;
        this.inType = inType;
        this.flushSize = flushSize;
        this.tablename = tablename;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        if (!bufferedElements.isEmpty()) {
            checkpointedState.addAll(bufferedElements);
            List<Tuple2<Result, Tuple2<String, IN>>> res = qf.queryHbase(t, bufferedElements);
            for (Object o : qf.transResult(res)) {
                queryResBuffer.add((OUT) o);
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor(
                "HbaseQueryRichMapFunction",
                inType
        );
        checkpointedState = context.getOperatorStateStore()
                .getListState(descriptor);
        if(context.isRestored()){
            for (Tuple2<String, IN> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

    /**
     * 处理每个元素
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(Tuple2<String, IN> value, Context ctx, Collector<OUT> out) throws Exception {
        // 每个元素在最后一次ckp之后10s后执行一次
        ctx.timerService().registerProcessingTimeTimer(
                new Date().getTime() + FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL() + 1000L);
        if (!queryResBuffer.isEmpty()) {
            queryResBuffer.forEach(x -> out.collect(x));
            queryResBuffer.clear();
        }
        if (!value.f0.isEmpty()) bufferedElements.add(value); // key不为空
        if (bufferedElements.size() >= flushSize) {
            // query hbase
            List<Tuple2<Result, Tuple2<String, IN>>> res = qf.queryHbase(t, bufferedElements);
            for (Object o : qf.transResult(res)) {
                out.collect((OUT) o);
            }
            bufferedElements.clear();
        }
    }

    /**
     * 当没有数据之后，触发最后一个查询结果.
     * 如果ckp卡主，这里也会延后，所以不会有gap问题
     *
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

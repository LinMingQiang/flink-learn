package com.flink.java.function.process;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.dbutil.FlinkHbaseFactory;
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
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @param <IN>  Tuple2<String, KafkaTopicOffsetMsgPoJo>
 * @param <OUT> WordCountPoJo
 */
public class HbaseQueryProcessFunction<IN, OUT> extends KeyedProcessFunction<String, IN, OUT>
        implements CheckpointedFunction {
    private Logger _log = LoggerFactory.getLogger(this.getClass());
    private int flushSize = 100;
    private ListState<IN> checkpointedState = null;// checkpoint state
    private List<IN> bufferedElements = new ArrayList<IN>(); // buffer List
    private List<OUT> queryResBuffer = new ArrayList<>(); // ckp的时候结果
    private AbstractHbaseQueryFunction<IN, OUT> qf = null;
    private TypeInformation<IN> inType = null;
    private Table t = null;
    private String tablename = null;
    public OutputTag<IN> queryEmpt = null; // 没查到的
    private List<IN> queryEmptList = new ArrayList<>();
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
        if(tablename != null)
            t = FlinkHbaseFactory.getTable(FlinkLearnPropertiesUtil.ZOOKEEPER(), tablename);
        _log.info("Init Hbase Success !");
    }

    /**
     *
     * @param qf
     * @param tablename
     * @param flushSize
     * @param inType
     * @param queryEmpt
     */
    public HbaseQueryProcessFunction(
            AbstractHbaseQueryFunction qf,
            String tablename,
            int flushSize,
            TypeInformation<IN> inType,
            OutputTag<IN> queryEmpt) {
        this.qf = qf;
        this.inType = inType;
        this.flushSize = flushSize;
        this.tablename = tablename;
        this.queryEmpt = queryEmpt;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        if (!bufferedElements.isEmpty()) {
            checkpointedState.addAll(bufferedElements);
                for (Tuple2<Result, IN> tmp : qf.queryHbase(t, bufferedElements)) {
                    if (tmp.f0 == null || tmp.f0.isEmpty()) {
                        queryEmptList.add(tmp.f1);
                    } else {
                        qf.transResult(tmp, queryResBuffer);
                    }
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
        if (context.isRestored()) {
            for (IN element : checkpointedState.get()) {
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
    public void processElement(IN value, Context ctx, Collector<OUT> out) throws Exception {
        // 每个元素在最后一次ckp之后10s后执行一次
        ctx.timerService().registerProcessingTimeTimer(
                new Date().getTime() + FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL() + 1000L);
        if (!queryResBuffer.isEmpty()) {
            queryResBuffer.forEach(x -> out.collect(x));
            queryResBuffer.clear();
        }
        if(!queryEmptList.isEmpty()){
            queryEmptList.forEach(x -> ctx.output(queryEmpt, x));
            queryEmptList.clear();
        }
        if (!qf.getRowkey(value).isEmpty()) {
            bufferedElements.add(value); // key不为空
        }
        if (bufferedElements.size() >= flushSize) {
            for (Tuple2<Result, IN> tmp : qf.queryHbase(t, bufferedElements)) {
                if (tmp.f0 == null || tmp.f0.isEmpty()) {
                    ctx.output(queryEmpt, tmp.f1);
                } else {
                    qf.transResult(tmp, queryResBuffer);
                }
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
        if(!queryEmptList.isEmpty()){
            queryEmptList.forEach(x -> ctx.output(queryEmpt, x));
            queryEmptList.clear();
        }
    }


}


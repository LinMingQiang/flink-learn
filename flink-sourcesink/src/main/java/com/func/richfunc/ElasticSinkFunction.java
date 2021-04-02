package com.func.richfunc;

import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.dbutil.ElasticsearchHandler7;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class ElasticSinkFunction extends RichSinkFunction<Row> implements CheckpointedFunction {
    TransportClient client = null;
    Logger LOG = LoggerFactory.getLogger(ElasticSinkFunction.class);
    private ListState<Row> checkpointedState = null;// checkpoint state
    private HashMap<String, Row> bufferedElements = new HashMap<String, Row>(); // buffer List
    Long nextTime = 0L;
    int invatel = 0;
    int size = 1000;
    String[] fieldNames;
    TypeInformation[] fieldTypes;

    public ElasticSinkFunction(String[] fieldNames,
                               TypeInformation[] fieldTypes,
                               int size, int invatel) {
        this.fieldNames=fieldNames;
        this.fieldTypes=fieldTypes;
        this.size = size;
        this.invatel = invatel;
        nextTime = System.currentTimeMillis() + invatel;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            super.open(parameters);
            ParameterTool parame = (ParameterTool) getRuntimeContext()
                    .getExecutionConfig()
                    .getGlobalJobParameters();
            FlinkLearnPropertiesUtil.init(parame);
            // 需要 env.registerCachedFile("hdfs:///path/to/your/file", "hdfsFile")
            // 需要 env.registerCachedFile("file:///path/to/your/file", "hdfsFile")
            File myFile = getRuntimeContext().getDistributedCache().getFile("es_pack_file");
            client = ElasticsearchHandler7.getGlobalEsClient(
                    FlinkLearnPropertiesUtil.ES_HOSTS(),
                    FlinkLearnPropertiesUtil.ES_CLUSTERNAME(),
                    FlinkLearnPropertiesUtil.ES_XPACK_PASSW(),
                    myFile.getPath());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Row value, Context context) throws Exception {
        String id = value.getField(1).toString();
        bufferedElements.put(id, value); // 每次都保存最新的
        if (System.currentTimeMillis() > nextTime || bufferedElements.size() > size) { // 每一分钟提交一次
            LOG.info("invoke commit : " + bufferedElements.toString());
            nextTime = System.currentTimeMillis() + 1000 * invatel;
            BulkRequestBuilder bulk = client.prepareBulk();
            bufferedElements.forEach((x, y) -> {
                try {
                    UpdateRequest updater = createUpdateReqest(value);
                    bulk.add(updater);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            commitBulk(bulk);
        }
    }


    /**
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        BulkRequestBuilder bulk = client.prepareBulk();
        bufferedElements.forEach((x, y) -> {
            try {
                checkpointedState.add(y);
                UpdateRequest updater = createUpdateReqest(y);
                bulk.add(updater);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        if(!bufferedElements.isEmpty()){
            LOG.info("snapshotState commit : " +bufferedElements.toString());
            commitBulk(bulk);
        }
    }

    /**
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor descriptor = new ListStateDescriptor(
                "buffered-elements",
                TypeInformation.of(Row.class));
//        descriptor.enableTimeToLive(StateTtlConfig
//                .newBuilder(Time.minutes(120)) // 2个小时
//                .updateTtlOnReadAndWrite() // 每次读取或者更新这个key的值的时候都对ttl做更新，所以清理的时间是 lastpdatetime + outtime
//                .cleanupFullSnapshot() // 创建完整快照时清理
//                .cleanupInRocksdbCompactFilter(100) // 达到100个过期就清理？
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .build());
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            checkpointedState.get().forEach(x -> {
                String id = x.getField(1).toString();
                bufferedElements.put(id, x);
                System.out.println("restore : " + x);
            });
        }
    }

    /**
     * 提交bulk
     *
     * @param bulk
     */
    public void commitBulk(BulkRequestBuilder bulk) {
        if (bulk.numberOfActions() > 0) {
            if (bulk.get().hasFailures()) { // 如果失败了不要clear
                System.out.println("ElasticReportSink : bulk fail " + bulk.get().buildFailureMessage());
            }
        }
        bufferedElements.clear();
    }

    /**
     * 第一个字段是indexname。第二个是 key。后面是字段了。
     * @param value
     * @return
     * @throws IOException
     */
    public UpdateRequest createUpdateReqest(Row value) throws IOException {
        String indexName = value.getField(0).toString();
        String id = value.getField(1).toString();
        XContentBuilder creatDoc = XContentFactory
                .jsonBuilder()
                .startObject();
        for (int i = 2; i < fieldTypes.length; i++) {
            if(fieldTypes[i].equals(Types.STRING)) {
                creatDoc.field(fieldNames[i], value.getField(i).toString()) ;
            } else if(fieldTypes[i].equals(Types.DOUBLE)) {
                creatDoc.field(fieldNames[i], Double.valueOf(value.getField(i).toString())) ;
            } else if (fieldTypes[i].equals(Types.LONG)) {
                creatDoc.field(fieldNames[i], Long.valueOf(value.getField(i).toString())) ;
            } else {
                creatDoc.field(fieldNames[i], Integer.valueOf(value.getField(i).toString())) ;
            }
        }
        creatDoc.endObject();
        UpdateRequest updater = new UpdateRequest(indexName, id)
                .upsert(creatDoc)
                .retryOnConflict(3)
                .doc(creatDoc);
        return updater;
    }

}

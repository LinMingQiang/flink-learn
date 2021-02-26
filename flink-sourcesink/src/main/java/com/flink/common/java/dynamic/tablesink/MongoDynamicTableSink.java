package com.flink.common.java.dynamic.tablesink;

import com.flink.common.java.dynamic.sinkfunc.MongoTableRichSinkFunction;
import com.flink.common.java.dynamic.sinkfunc.PrintlnRetractRichSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class MongoDynamicTableSink implements DynamicTableSink {
    private DataType type;
    private ReadableConfig options;
    private DataType shcema;
    public MongoDynamicTableSink(DataType type, ReadableConfig options, DataType shcema) {
        this.type = type;
        this.options = options;
        this.shcema =shcema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter = context.createDataStructureConverter(type);
        return SinkFunctionProvider.of(new MongoTableRichSinkFunction(converter, options, shcema));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongoDynamicTableSink(type, options, shcema);
    }

    @Override
    public String asSummaryString() {
        return "asSummaryString";
    }
}

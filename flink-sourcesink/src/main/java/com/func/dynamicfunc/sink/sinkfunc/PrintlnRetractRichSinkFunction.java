package com.func.dynamicfunc.sink.sinkfunc;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

public class PrintlnRetractRichSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private final DynamicTableSink.DataStructureConverter converter;
    private final PrintSinkOutputWriter<String> writer;

    public PrintlnRetractRichSinkFunction(
            DynamicTableSink.DataStructureConverter converter, String printIdentifier, boolean stdErr) {
        this.converter = converter;
        this.writer = new PrintSinkOutputWriter<>(printIdentifier, stdErr);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
        writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(RowData value, Context context) {
        String rowKind = value.getRowKind().shortString();
        Object data = converter.toExternal(value);
        writer.write("Custom Println :" + rowKind + "(" + data + ")");
    }
}

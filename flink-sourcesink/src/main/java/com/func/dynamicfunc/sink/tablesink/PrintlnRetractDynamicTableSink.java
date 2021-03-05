package com.func.dynamicfunc.sink.tablesink;

import com.func.dynamicfunc.sink.sinkfunc.PrintlnRetractRichSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

public class PrintlnRetractDynamicTableSink implements DynamicTableSink {
    private final DataType type;
    private final String printIdentifier;
    private final boolean stdErr;

    public PrintlnRetractDynamicTableSink(DataType type, String printIdentifier, boolean stdErr) {
        this.type = type;
        this.printIdentifier = printIdentifier;
        this.stdErr = stdErr;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context)
    {
        DataStructureConverter converter = context.createDataStructureConverter(type);
        return SinkFunctionProvider.of(new PrintlnRetractRichSinkFunction(converter, printIdentifier, stdErr));
    }

    @Override
    public DynamicTableSink copy() {
        return new PrintlnRetractDynamicTableSink(type, printIdentifier, stdErr);
    }

    @Override
    public String asSummaryString() {
        return "Print to " + (stdErr ? "System.err" : "System.out");
    }
}

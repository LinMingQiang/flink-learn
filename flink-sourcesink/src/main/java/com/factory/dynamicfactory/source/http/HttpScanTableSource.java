package com.factory.dynamicfactory.source.http;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.types.DataType;

public class HttpScanTableSource implements ScanTableSource {
    private ReadableConfig options;
    private DataType shcema;

    public HttpScanTableSource(ReadableConfig options, DataType shcema) {
        this.options = options;
        this.shcema = shcema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        return SourceFunctionProvider.of(new HttpRichSourceFunction(options, shcema), false);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}

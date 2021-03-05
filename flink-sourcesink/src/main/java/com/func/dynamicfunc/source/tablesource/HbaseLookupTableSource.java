package com.func.dynamicfunc.source.tablesource;

import com.func.dynamicfunc.source.tablefunc.HbaseTableFunc;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

public class HbaseLookupTableSource implements ScanTableSource,LookupTableSource {

    // tableFunction
    // LookupRuntimeProvider的实现有两个，一个是同步，一个是异步
    // 同步：TableFunction， 异步:AsyncTableFunction
    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {

        return TableFunctionProvider.of(new HbaseTableFunc());
        // return AsyncTableFunctionProvider.of(new HbaseAsyncTableFunc());
    }

    @Override
    public DynamicTableSource copy() {
        return new HbaseLookupTableSource();
    }

    @Override
    public String asSummaryString() {
        return "hbase lookup:";
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    // sourceFunction
    // 两种读取方式，实现的方式不同
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
    }
}

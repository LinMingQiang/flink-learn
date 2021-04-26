package com.factory.dynamicfactory.source;

// TableSoure有两种，一种是scan，一种是lookup, lookup是join的时候去读取，kafka是scan读取全部

import com.func.dynamicfunc.source.tablesource.HbaseLookupTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.Set;

public class HbaseLookupTableSourceFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "custom-hbase-lookup";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        return new HbaseLookupTableSource();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }
}

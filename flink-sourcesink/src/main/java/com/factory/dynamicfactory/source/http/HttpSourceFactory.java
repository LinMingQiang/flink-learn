package com.factory.dynamicfactory.source.http;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class HttpSourceFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "http-source";
    public static final ConfigOption<String> HTTPURL = key("url")
            .stringType()
            .noDefaultValue();
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        final DataType physicalDataType =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new HttpScanTableSource(options, physicalDataType);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet set = new HashSet<>();
        set.add(HTTPURL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}

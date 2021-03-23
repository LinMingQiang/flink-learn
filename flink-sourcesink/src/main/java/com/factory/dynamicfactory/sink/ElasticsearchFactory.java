package com.factory.dynamicfactory.sink;

import com.func.dynamicfunc.sink.tablesink.ElasticsearchDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class ElasticsearchFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "custom-es";
    public static final ConfigOption<String> ES_URL = key("es.address")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ES_CLUSTERNAME = key("es.clustername")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ES_PASSW = key("es.passw")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> ES_INDEX = key("es.index")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Long> ES_COMMIT_SIZE = key("es.commit.size")
            .longType()
            .noDefaultValue();

    public static final ConfigOption<Long> ES_COMMIT_INTERVAL = key("es.commit.intervalsec")
            .longType()
            .noDefaultValue();
    public static final ConfigOption<String> ES_ROWKEY = key("es.rowkey")
            .stringType()
            .noDefaultValue();
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        final DataType shcema =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new ElasticsearchDynamicTableSink(
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                options,
                shcema);    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ES_URL);
        options.add(ES_CLUSTERNAME);
        options.add(ES_INDEX);
        options.add(ES_PASSW);
        options.add(ES_COMMIT_SIZE);
        options.add(ES_COMMIT_INTERVAL);
        options.add(ES_ROWKEY);
        return options;
    }
}

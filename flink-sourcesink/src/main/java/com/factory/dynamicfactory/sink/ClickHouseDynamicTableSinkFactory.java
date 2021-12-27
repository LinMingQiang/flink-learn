package com.factory.dynamicfactory.sink;

import com.func.dynamicfunc.sink.tablesink.ClickhouseDynamicTableSink;
import com.func.dynamicfunc.sink.tablesink.MySqlDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class ClickHouseDynamicTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "clickhouse";
    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc database url.");
    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc table name.");
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc user name.");
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("the jdbc password.");
    // write config options
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "the flush max size (includes all append, upsert and delete records), over this number"
                                    + " of records, will flush data. The default value is 100.");
    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "the flush interval mills, over this time, asynchronous threads will flush data. The "
                                    + "default value is 1s.");
    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        final DataType shcema =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new ClickhouseDynamicTableSink(
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
        options.add(URL);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLE_NAME);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        return options;
    }
}

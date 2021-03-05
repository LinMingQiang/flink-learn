package com.factory.dynamicfactory.sink;

import com.func.dynamicfunc.sink.tablesink.PrintlnRetractDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class PrintlnRetractDynamicTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "printRetract";
    public static final ConfigOption<String> PRINT_IDENTIFIER = key("print-identifier")
            .stringType()
            .noDefaultValue()
            .withDescription("Message that identify print and is prefixed to the output of the value.");

    public static final ConfigOption<Boolean> STANDARD_ERROR = key("standard-error")
            .booleanType()
            .defaultValue(false)
            .withDescription("True, if the format should print to standard error instead of standard out.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new PrintlnRetractDynamicTableSink(
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                options.get(PRINT_IDENTIFIER),
                options.get(STANDARD_ERROR));
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * 必须配置
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    /**
     * 可选配置
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRINT_IDENTIFIER);
        options.add(STANDARD_ERROR);
        return options;    }
}

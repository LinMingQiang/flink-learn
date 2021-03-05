package com.factory.dynamicfactory.sink;

import com.func.dynamicfunc.sink.tablesink.MongoDynamicTableSink;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

public class MongoDynamicTableSinkFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "custom-mongo";
    public static final ConfigOption<String> MONGO_URL = key("mongourl")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> MONGO_USER = key("mongouser")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> MONGO_PASSW = key("mongopassw")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> MONGO_COLLECTION = key("collection")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> MONGO_DB = key("mongodb")
            .stringType()
            .noDefaultValue();
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        final DataType shcema =
                context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new MongoDynamicTableSink(
                context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                options,
                shcema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    /**
     * 必须配置
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    /**
     * 可选配置
     *
     * @return
     */
    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MONGO_URL);
        options.add(MONGO_USER);
        options.add(MONGO_PASSW);
        options.add(MONGO_COLLECTION);
        options.add(MONGO_DB);

        return options;
    }

    //  override def createStreamTableSink(properties: ju.Map[String, String])
    //  : StreamTableSink[tuple.Tuple2[lang.Boolean, Row]] = {
    //    val descriptorProperties = getValidatedProperties(properties)
    //    val tableSchema = descriptorProperties.getTableSchema(SCHEMA)
    //    val fieldNames = tableSchema.getFieldNames
    //    val fieldTypes = tableSchema.getFieldDataTypes
    //
    //    val props = new mutable.HashMap[String, String]()
    //    props.put("connector.database", descriptorProperties.getString(CONNECTOR_DATABASE))
    //    props.put("connector.collection", descriptorProperties.getString(CONNECTOR_COLLECTION))
    //    props.put("connector.host", descriptorProperties.getString(CONNECTOR_HOST))
    //    props.put(CONNECTOR_PASSW, descriptorProperties.getString(CONNECTOR_PASSW))
    //    props.put(CONNECTOR_USER, descriptorProperties.getString(CONNECTOR_USER))
    //
    //    new CommonUpsertStreamTableSink(tableSchema, new MongoDynamicTableSink(props, fieldNames, fieldTypes))
    //  }
}

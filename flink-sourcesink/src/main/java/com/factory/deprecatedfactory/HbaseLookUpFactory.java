package com.factory.deprecatedfactory;

import com.func.udffunc.HbaseLookupFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.*;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
import com.func.depfun.tablesource.HbaseAyscLookupTableSource;
@Deprecated
public class HbaseLookUpFactory implements StreamTableSourceFactory<Row> {
    @Override
    public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(
                descriptorProperties.getTableSchema(SCHEMA));

        return new HbaseAyscLookupTableSource(schema,
                HbaseLookupFunction.builder()
                        .setFieldNames(schema.getFieldNames())
                        .setFieldTypes(schema.getFieldTypes())
                        .build(),
                null);
    }

    @Override
    public Map<String, String> requiredContext() {
        HashMap<String, String> context = new HashMap<String, String>();
        context.put(CONNECTOR_TYPE, "hbaselookup");
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        ArrayList properties = new ArrayList<>();
        // update mode
        properties.add(UPDATE_MODE);
        properties.add(FORMAT + ".*"); // 用以支持format的配置，当用connect的时候需要，否则报错
        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
        properties.add(CONNECTOR_PROPERTIES + ".*");
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        return properties;
    }
}

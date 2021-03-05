package com.factory.deprecatedfactory;

import com.func.depfun.tablesink.PrintlnAppendStreamTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.KafkaValidator.*;
import static org.apache.flink.table.descriptors.KafkaValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.Schema.*;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;
@Deprecated

public class PrintlnAppendStreamFactory implements StreamTableSinkFactory<Row> {
    @Override
    public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
       // bridge to java.sql.Timestamp/Time/Date
        DataType[] fieldTypes = Arrays.stream(tableSchema.getFieldDataTypes())
                .map(dt -> {
                    switch (dt.getLogicalType().getTypeRoot()) {
                        case TIMESTAMP_WITHOUT_TIME_ZONE:
                            return dt.bridgedTo(Timestamp.class);
                        case TIME_WITHOUT_TIME_ZONE:
                            return dt.bridgedTo(Time.class);
                        case DATE:
                            return dt.bridgedTo(Date.class);
                        default:
                            return dt;
                    }
                })
                .toArray(DataType[]::new);
        return new PrintlnAppendStreamTableSink(tableSchema.getFieldNames(), fieldTypes);
    }
    /**
     * 必须要有此context 里面的 key(CONNECTOR_TYPE) 的 配置才会匹配到此 factory
     * @return
     */
    @Override
    public Map<String, String> requiredContext() {
        HashMap<String, String> context = new HashMap<String, String>();
        context.put(CONNECTOR_TYPE, "printsink");
        context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
        return context;
    }

    /**
     * 支持的配置，不在这里面的会报错
     * @return
     */
    @Override
    public List<String> supportedProperties() {
        ArrayList properties = new ArrayList<>();
        // update mode
        properties.add(UPDATE_MODE);

        properties.add("println.prefix");
        // schema
        properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
        properties.add(SCHEMA + ".#." + SCHEMA_NAME);
        properties.add(FORMAT + ".*"); // 用以支持format的配置，当用connect的时候需要，否则报错
        properties.add(CONNECTOR_PROPERTIES);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
        properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
        properties.add(CONNECTOR_PROPERTIES + ".*");
        return properties;
    }
}

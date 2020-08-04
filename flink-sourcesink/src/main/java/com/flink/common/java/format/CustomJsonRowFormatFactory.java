package com.flink.common.java.format;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomJsonRowFormatFactory extends TableFormatFactoryBase<Row>
        implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

    public CustomJsonRowFormatFactory(String type, int version, boolean supportsSchemaDerivation) {
        super(type, version, supportsSchemaDerivation);
    }
    public CustomJsonRowFormatFactory() {
        super(CustomJsonValidator.FORMAT_TYPE_VALUE, 1, true);
    }


    /**
     * 反序列化
     * @param properties
     * @return
     */
    @Override
    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        final CustomJsonRowDeserializationSchema.Builder schema =
                new CustomJsonRowDeserializationSchema.Builder(createTypeInformation(descriptorProperties));

        descriptorProperties.getOptionalBoolean(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
                .ifPresent(flag -> {
                    if (flag) {
                        schema.failOnMissingField();
                    }
                });

        return schema.build();
    }

    /**
     * 序列化
     * @param properties
     * @return
     */
    @Override
    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);

        // create and configure
        return new CustomJsonRowSerializationSchema.Builder(createTypeInformation(descriptorProperties)).build();
    }




    @Override
    protected List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(JsonValidator.FORMAT_JSON_SCHEMA);
        properties.add(JsonValidator.FORMAT_SCHEMA);
        properties.add(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD);
        return properties;
    }

    /**
     * 校验
     * @param propertiesMap
     * @return
     */
    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties();
        descriptorProperties.putProperties(propertiesMap);

        // validate
        new JsonValidator().validate(descriptorProperties);

        return descriptorProperties;
    }



    private TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        if (descriptorProperties.containsKey(JsonValidator.FORMAT_SCHEMA)) {
            return (RowTypeInfo) descriptorProperties.getType(JsonValidator.FORMAT_SCHEMA);
        } else if (descriptorProperties.containsKey(JsonValidator.FORMAT_JSON_SCHEMA)) {
            return JsonRowSchemaConverter.convert(descriptorProperties.getString(JsonValidator.FORMAT_JSON_SCHEMA));
        } else {
            return deriveSchema(descriptorProperties.asMap()).toRowType();
        }
    }
}

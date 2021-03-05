package com.format.csvformat;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.format.csvformat.CsvCustomRowDeserializationSchema.Builder;
import org.apache.flink.table.descriptors.CsvValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

public class CsvCustomRowFormatFactory extends TableFormatFactoryBase<Row> implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {
    public CsvCustomRowFormatFactory() {
        super("custom-csv", 1, true);
    }

    public List<String> supportedFormatProperties() {
        List<String> properties = new ArrayList();
        properties.add("format.field-delimiter");
        properties.add("format.line-delimiter");
        properties.add("format.disable-quote-character");
        properties.add("format.quote-character");
        properties.add("format.allow-comments");
        properties.add("format.ignore-parse-errors");
        properties.add("format.array-element-delimiter");
        properties.add("format.escape-character");
        properties.add("format.null-literal");
        properties.add("format.schema");
        return properties;
    }

    public DeserializationSchema<Row> createDeserializationSchema(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        Builder schemaBuilder = new Builder(createTypeInformation(descriptorProperties));
        descriptorProperties.getOptionalCharacter("format.field-delimiter").ifPresent(schemaBuilder::setFieldDelimiter);
        descriptorProperties.getOptionalCharacter("format.quote-character").ifPresent(schemaBuilder::setQuoteCharacter);
        descriptorProperties.getOptionalBoolean("format.allow-comments").ifPresent(schemaBuilder::setAllowComments);
        descriptorProperties.getOptionalBoolean("format.ignore-parse-errors").ifPresent(schemaBuilder::setIgnoreParseErrors);
        descriptorProperties.getOptionalString("format.array-element-delimiter").ifPresent(schemaBuilder::setArrayElementDelimiter);
        descriptorProperties.getOptionalCharacter("format.escape-character").ifPresent(schemaBuilder::setEscapeCharacter);
        descriptorProperties.getOptionalString("format.null-literal").ifPresent(schemaBuilder::setNullLiteral);
        return schemaBuilder.build();
    }

    public SerializationSchema<Row> createSerializationSchema(Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        org.apache.flink.formats.csv.CsvRowSerializationSchema.Builder schemaBuilder = new org.apache.flink.formats.csv.CsvRowSerializationSchema.Builder(createTypeInformation(descriptorProperties));
        descriptorProperties.getOptionalCharacter("format.field-delimiter").ifPresent(schemaBuilder::setFieldDelimiter);
        descriptorProperties.getOptionalString("format.line-delimiter").ifPresent(schemaBuilder::setLineDelimiter);
        if ((Boolean)descriptorProperties.getOptionalBoolean("format.disable-quote-character").orElse(false)) {
            schemaBuilder.disableQuoteCharacter();
        } else {
            descriptorProperties.getOptionalCharacter("format.quote-character").ifPresent(schemaBuilder::setQuoteCharacter);
        }

        descriptorProperties.getOptionalString("format.array-element-delimiter").ifPresent(schemaBuilder::setArrayElementDelimiter);
        descriptorProperties.getOptionalCharacter("format.escape-character").ifPresent(schemaBuilder::setEscapeCharacter);
        descriptorProperties.getOptionalString("format.null-literal").ifPresent(schemaBuilder::setNullLiteral);
        return schemaBuilder.build();
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(propertiesMap);
        (new CsvValidator()).validate(descriptorProperties);
        return descriptorProperties;
    }

    private static TypeInformation<Row> createTypeInformation(DescriptorProperties descriptorProperties) {
        return (TypeInformation)(descriptorProperties.containsKey("format.schema") ? (RowTypeInfo)descriptorProperties.getType("format.schema") : deriveSchema(descriptorProperties.asMap()).toRowType());
    }
}


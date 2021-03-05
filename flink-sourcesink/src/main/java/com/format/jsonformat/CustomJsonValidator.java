package com.format.jsonformat;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

public class CustomJsonValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "custom-json";
    public static final String FORMAT_SCHEMA = "format.schema";
    public static final String FORMAT_JSON_SCHEMA = "format.json-schema";
    public static final String FORMAT_FAIL_ON_MISSING_FIELD = "format.fail-on-missing-field";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateBoolean(FORMAT_DERIVE_SCHEMA, true);
        final boolean deriveSchema = properties.getOptionalBoolean(FORMAT_DERIVE_SCHEMA).orElse(true);
        final boolean hasSchema = properties.containsKey(FORMAT_SCHEMA);
        final boolean hasSchemaString = properties.containsKey(FORMAT_JSON_SCHEMA);
        // if a schema is defined, no matter derive schema is set or not, will use the defined schema
        if (!deriveSchema && hasSchema && hasSchemaString) {
            throw new ValidationException("A definition of both a schema and JSON schema is not allowed.");
        } else if (!deriveSchema && !hasSchema && !hasSchemaString) {
            throw new ValidationException("A definition of a schema or JSON schema is required " +
                    "if derivation from table's schema is disabled.");
        } else if (hasSchema) {
            properties.validateType(FORMAT_SCHEMA, false, true);
        } else if (hasSchemaString) {
            properties.validateString(FORMAT_JSON_SCHEMA, false, 1);
        }

        properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, true);
    }
}

package com.flink.sql.common.format;

import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;

public class ConnectorFormatDescriptorUtils {

    /** @return */
    public static Json kafkaConnJsonFormat() {
        return new Json().failOnMissingField(false);
    }

    /** */
    public static Csv kafkaConnCsvFormat() {
        return new Csv().deriveSchema().fieldDelimiter(',').lineDelimiter("\n").ignoreParseErrors();
    }
}

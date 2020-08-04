package com.flink.sql.common.format;

import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;

public class ConnectorFormatDescriptorUtils {


    /**
     * @return
     */
    public static Json kafkaConnJsonFormat() {
        return new Json()
                .failOnMissingField(false);
    }


    /**
     */
    public static Csv kafkaConnCsvFormat() {
        return new Csv()
                .deriveSchema()
                .fieldDelimiter(',')
                .lineDelimiter("\n")
                .ignoreParseErrors();

    }
}

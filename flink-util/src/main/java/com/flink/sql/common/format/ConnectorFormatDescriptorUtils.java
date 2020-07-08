package com.flink.sql.common.format;

import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;

public class ConnectorFormatDescriptorUtils {


    /**
     * @param kafkaConnector
     * @return
     */
    public static Json kafkaConnJsonFormat(Kafka kafkaConnector) {
        return new Json()
                .failOnMissingField(false);
    }


    /**
     * @param kafkaConnector
     */
    public static Csv kafkaConnCsvFormat(Kafka kafkaConnector) {
        return new Csv()
                .deriveSchema()
                .fieldDelimiter(',')
                .lineDelimiter("\n")
                .ignoreParseErrors();

    }
}

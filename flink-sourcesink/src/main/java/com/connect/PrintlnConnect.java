package com.connect;

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class PrintlnConnect extends ConnectorDescriptor {
    private Map<String, String> printProperties = new HashMap<>();

    /**
     * Constructs a {@link ConnectorDescriptor}.
     *
     * @param type         string that identifies this connector
     * @param version      property version for backwards compatibility
     * @param formatNeeded flag for basic validation of a needed format descriptor
     */
    public PrintlnConnect(String type, int version, boolean formatNeeded) {
        super(type, version, formatNeeded);
    }
    /**
     * Connector descriptor for the Apache Kafka message queue.
     */
    public PrintlnConnect() {
        super("printsink", 1, true);
    }



    @Override
    protected Map<String, String> toConnectorProperties() {
        return printProperties;
    }


    /**
     *
     * @param key
     * @param value
     * @return
     */
    public PrintlnConnect property(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        if (this.printProperties == null) {
            this.printProperties = new HashMap<>();
        }
        printProperties.put(key, value);
        return this;
    }
}

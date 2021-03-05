package com.connect;

import org.apache.flink.table.descriptors.ConnectorDescriptor;

import java.util.HashMap;
import java.util.Map;

public class HbaseLookUpConnect  extends ConnectorDescriptor {
    /**
     * Constructs a {@link ConnectorDescriptor}.
     *
     * @param type         string that identifies this connector
     * @param version      property version for backwards compatibility
     * @param formatNeeded flag for basic validation of a needed format descriptor
     */
    public HbaseLookUpConnect(String type, int version, boolean formatNeeded) {
        super(type, version, formatNeeded);
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return new HashMap<>();
    }


}

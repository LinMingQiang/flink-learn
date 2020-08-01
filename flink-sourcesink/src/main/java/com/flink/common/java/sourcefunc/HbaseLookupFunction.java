package com.flink.common.java.sourcefunc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.*;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class HbaseLookupFunction extends TableFunction<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    public HbaseLookupFunction(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @param keys join的key
     */
    public void eval(Object... keys) {
        for (Object o : keys) {
            System.out.println(o);
        }
        if (keys[0].equals("1")) {
            Row keyRow = new Row(3);
            keyRow.setField(0, "id1");
            keyRow.setField(1, 1L);
            keyRow.setField(2, "hbaseValue");
            collect(keyRow);
        }
    }

    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    /**
     * Builder for a {@link JDBCLookupFunction}.
     */
    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        /**
         * required, field names of this jdbc table.
         */
        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /**
         * required, field types of this jdbc table.
         */
        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }


        public HbaseLookupFunction build() {
            return new HbaseLookupFunction(fieldNames, fieldTypes);
        }

    }
}

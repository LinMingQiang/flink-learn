package com.flink.common.java.tablesource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class HbaseAyscLookupTableSource implements LookupableTableSource<Row>, StreamTableSource<Row> {

    public String[] fieldNames = null;
    public DataType[] fieldTypes = null;
    public TableFunction<Row> func = null;
    public AsyncTableFunction<Row> asyncFunc = null;
    public HbaseAyscLookupTableSource(String[] fieldNames,
                                      DataType[] fieldTypes,
                                      TableFunction<Row> func,
                                      AsyncTableFunction<Row> asyncFunc) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.func = func;
        this.asyncFunc = asyncFunc;
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return func;
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return asyncFunc;
    }

    @Override
    public boolean isAsyncEnabled() {
        return asyncFunc != null;
    }

    @Override
    public boolean isBounded() {
        return false;
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment streamExecutionEnvironment) {
        return null;
    }

    @Override
    public DataType getProducedDataType() {
        DataTypes.Field[] a = new DataTypes.Field[fieldNames.length + 1];
        for (int i = 0; i < fieldNames.length; i++) {
            a[i] = (DataTypes.FIELD(fieldNames[i], fieldTypes[i]));
        }
        return DataTypes.ROW(a);
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema
                .builder()
                .fields(fieldNames, fieldTypes)
                .build();
    }

    @Override
    public String explainSource() {
        return null;
    }


    public static final class Builder {
        private String[] fieldNames;
        private String[] connectionField;
        private TypeInformation[] fieldTypes;

        private Builder() {
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder withConnectionField(String[] connectionField) {
            this.connectionField = connectionField;
            return this;
        }

//        public HbaseAyscLookupTableSource build() {
//            return new HbaseAyscLookupTableSource(fieldNames,connectionField, fieldTypes);
//        }
    }
}

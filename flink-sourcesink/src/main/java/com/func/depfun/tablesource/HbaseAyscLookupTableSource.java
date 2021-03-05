package com.func.depfun.tablesource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * 维表
 */
@Deprecated
public class HbaseAyscLookupTableSource implements StreamTableSource<Row>, LookupableTableSource<Row> {

    public TableSchema schema;
    public TableFunction<Row> func = null;
    public AsyncTableFunction<Row> asyncFunc = null;

    public HbaseAyscLookupTableSource(TableSchema schema,
                                      TableFunction<Row> func,
                                      AsyncTableFunction<Row> asyncFunc) {
        this.schema=schema;
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

//    @Override
//    public boolean isBounded() {
//        return false;
//    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(getTableSchema().getFieldTypes(),getTableSchema().getFieldNames());
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String explainSource() {
        return null;
    }

    /***
     * @param execEnv
     * @return
     */
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        throw new UnsupportedOperationException("do not support getDataStream");
    }


    public static final class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private static Builder Builder() {

            return new Builder();
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


//        public HbaseAyscLookupTableSource build() {
//            return new HbaseAyscLookupTableSource(fieldNames,connectionField, fieldTypes);
//        }
    }
}

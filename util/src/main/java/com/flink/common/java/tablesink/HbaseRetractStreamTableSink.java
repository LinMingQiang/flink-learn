package com.flink.common.java.tablesink;

import com.google.common.cache.LoadingCache;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class HbaseRetractStreamTableSink implements RetractStreamTableSink<Row> {

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public DataType getConsumedDataType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation[0];
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return null;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }
}

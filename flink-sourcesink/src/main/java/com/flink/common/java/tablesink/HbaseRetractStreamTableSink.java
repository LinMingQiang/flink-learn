package com.flink.common.java.tablesink;

import com.flink.common.java.sinkfunc.HbaseAsyncSinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * 有些方法不要乱重写。。。 例如 ： getConsumedDataType
 */
public class HbaseRetractStreamTableSink implements RetractStreamTableSink<Row> {
    private String[] fieldNames;
    private DataType[] fieldTypes;
    private TableSchema ts = null;
    public HbaseRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        getTableSchema();
    }

    @Override
    public TableSchema getTableSchema() {
        if(ts == null)
        this.ts = new TableSchema.Builder().fields(fieldNames, fieldTypes).build();
        return ts;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new HbaseAsyncSinkFunction(1000)).name(this.getClass().getSimpleName());
    }
    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return getTableSchema().getFieldTypes();
    }
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        ts = new TableSchema.Builder().fields(fieldNames, fromLegacyInfoToDataType(fieldTypes)).build();
        this.fieldTypes = ts.getFieldDataTypes();
        this.fieldNames = ts.getFieldNames();
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(getTableSchema().getFieldTypes(),getTableSchema().getFieldNames());
    }
}

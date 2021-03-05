package com.func.depfun.tablesink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

@Deprecated
public class PrintlnRetractStreamTableSink implements RetractStreamTableSink<Row> {
    private String[] fieldNames;
    private DataType[] fieldTypes;
    private TableSchema ts = null;

    public PrintlnRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    public PrintlnRetractStreamTableSink() {

    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(getTableSchema().getFieldTypes(),getTableSchema().getFieldNames());
    }
    @Override
    public TableSchema getTableSchema() {
        if(ts == null)
            this.ts = new TableSchema.Builder().fields(fieldNames, fieldTypes).build();
        return ts;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.filter(x -> x.f0).addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                System.out.println("Factory Print : " + value.f1);
            }
        }).name(this.getClass().getSimpleName());
    }
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return this;
    }
}
